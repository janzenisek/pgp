using ILGPU;
using ILGPU.Algorithms;
using ILGPU.Runtime;
using PGP.Data;
using System;
using System.Collections.Generic;
using System.Linq;

namespace PGP.Core.Operators {

  // ===============================================================================================
  // Data-parallel (GPU) program evaluator.
  //
  // Evaluation.EvaluateStack / EvaluateProgram exploit POPULATION-based parallelism: many CPU
  // threads each evaluate a *different* program (see RunParallel/RunParallelDeterministic in
  // Algorithm.cs). EvaluateGPU instead exploits DATA-based parallelism: a *single* program is
  // compiled once into a flat op-code array, and one GPU thread evaluates one data row.
  //
  // WHY DATA-BASED AND NOT POPULATION-BASED ON THE GPU?
  // Every individual in a GP population has a different tree shape, so "one GPU thread per
  // individual" would need a different kernel (or at least a different code path) per individual,
  // recompiled/re-dispatched every generation - GPU kernel launch and host<->device transfer
  // overhead (~0.05-0.5 ms) would then dominate, and it would compete with (rather than
  // complement) the CPU parallelism already used across the population. Evaluating a *single*
  // program across all rows with one thread per row keeps a single, reusable generic kernel and
  // turns the whole dataset into the parallel work unit instead.
  //
  // WHEN IS IT WORTH IT?
  // Only once RowCount is large enough (roughly >= 10^4-10^5 rows) to amortize per-call kernel
  // launch/transfer overhead. For small datasets (e.g. the ~1000-row samples in Program.cs) the
  // CPU evaluators (EvaluateStack/EvaluateProgram) will typically still be faster - use EvaluateGPU
  // only for large-row-count workloads, and benchmark before switching pgp.Evaluate over to it.
  //
  // SUPPORTED OPERATORS
  // Only a subset of Functions.All is implemented on the GPU kernel (the ones that are simple,
  // branch-light, and don't rely on unprotected math that could throw). Any program containing an
  // unsupported operator (e.g. raw Division/Logarithm/Exponential) transparently falls back to
  // Evaluation.EvaluateStack so correctness is never compromised.
  // ===============================================================================================
  public static class EvaluationGpu {

    private const byte OpConstant = 0;
    private const byte OpVariable = 1;
    private const byte OpAdd = 10;
    private const byte OpSub = 11;
    private const byte OpMul = 12;
    private const byte OpAnalyticQuotient = 13;
    private const byte OpSin = 14;
    private const byte OpCos = 15;
    private const byte OpTan = 16;
    private const byte OpTanh = 17;
    private const byte OpProtectedLog = 18;
    private const byte OpProtectedExp = 19;
    private const byte OpPi = 20;

    private static readonly Dictionary<string, byte> _opcodeBySymbol = new() {
      { Functions.Addition.Symbol, OpAdd },
      { Functions.Subtraction.Symbol, OpSub },
      { Functions.Multiplication.Symbol, OpMul },
      { Functions.AnalyticQuotient.Symbol, OpAnalyticQuotient },
      { Functions.Sine.Symbol, OpSin },
      { Functions.Cosine.Symbol, OpCos },
      { Functions.Tangent.Symbol, OpTan },
      { Functions.HyperbolicTangent.Symbol, OpTanh },
      { Functions.ProtectedLogarithm.Symbol, OpProtectedLog },
      { Functions.ProtectedExponential.Symbol, OpProtectedExp },
      { Functions.Pi.Symbol, OpPi },
    };

    // ---------------------------------------------------------------------------------------------
    // Threading model: ONE ACCELERATOR PER CPU WORKER THREAD (not per individual).
    //
    // A single physical device only has one hardware command queue's worth of real parallelism
    // anyway, so creating an Accelerator per *individual* (hundreds/thousands of times per run)
    // would just add repeated, expensive context-creation overhead (tens-hundreds of ms each) for
    // no concurrency benefit. Sharing ONE Accelerator across all threads behind a single lock (the
    // previous version) avoids the overhead, but then a Parallel.For population loop calling
    // Evaluate serializes ALL GPU work behind that lock plus a possibly stalling OpenCL driver -
    // which is what produced the apparent "deadlock".
    //
    // The practical middle ground: create one Accelerator (and one cached device data buffer) per
    // CPU worker thread, lazily, the first time that thread calls EvaluateGPU. The number of such
    // accelerators is bounded by the degree of CPU parallelism (a handful), not by population size
    // or generation count, and each thread's calls proceed without contending on a shared lock.
    // ---------------------------------------------------------------------------------------------
    private static readonly object _initLock = new();
    private static Context? _context;
    private static Device? _device;
    private static volatile bool _gpuDisabled; // tripped permanently if the driver hangs/misbehaves

    private sealed class ThreadGpuContext {
      public required Accelerator Accelerator;
      public required Action<
        Index1D,
        ArrayView<double>, int,
        ArrayView<byte>,
        ArrayView<double>,
        ArrayView<int>,
        int,
        int,
        ArrayView<double>> Kernel;
      public DataRecord? CachedDataRecordRef;
      public MemoryBuffer1D<double, Stride1D.Dense>? CachedDataBuffer;
    }

    private static readonly ThreadLocal<ThreadGpuContext?> _threadContext = new(() => CreateThreadContext());

    // Watchdog timeout for a single kernel launch + synchronize. Some OpenCL drivers (integrated
    // GPUs in particular) can stall indefinitely instead of returning an error; without a timeout
    // that stall would hang the whole GP run. On timeout, GPU evaluation is disabled for the rest
    // of the process and every subsequent call falls back to the CPU evaluator.
    private static readonly TimeSpan KernelTimeout = TimeSpan.FromSeconds(5);

    // True once a usable ILGPU accelerator (GPU or, as a last resort, ILGPU's CPU accelerator)
    // has been created for the calling thread. EvaluateGPU() falls back to the CPU evaluator
    // automatically if this is false, so callers don't need to check it themselves.
    public static bool IsAvailable => !_gpuDisabled && _threadContext.Value != null;

    private static void EnsureDeviceSelected() {
      if (_device != null || _gpuDisabled) return;
      lock (_initLock) {
        if (_device != null || _gpuDisabled) return;
        try {
          _context = Context.Create(builder => builder.Default().EnableAlgorithms());
          // Prefer a real GPU device; fall back to ILGPU's software CPU accelerator so programs
          // relying on EvaluateGPU still run (functionally, not for speed) without a GPU present.
          _device = _context.Devices.FirstOrDefault(d => d.AcceleratorType != AcceleratorType.CPU)
                    ?? _context.Devices.FirstOrDefault();
          if (_device == null) _gpuDisabled = true;
        } catch {
          _context = null;
          _device = null;
          _gpuDisabled = true;
        }
      }
    }

    private static ThreadGpuContext? CreateThreadContext() {
      EnsureDeviceSelected();
      if (_gpuDisabled || _device == null || _context == null) return null;

      try {
        var accelerator = _device.CreateAccelerator(_context);
        var kernel = accelerator.LoadAutoGroupedStreamKernel<
          Index1D, ArrayView<double>, int, ArrayView<byte>, ArrayView<double>, ArrayView<int>, int, int, ArrayView<double>>(
          EvaluateRowKernel);
        return new ThreadGpuContext { Accelerator = accelerator, Kernel = kernel };
      } catch {
        // No usable ILGPU backend on this thread/device (missing/unsupported driver, etc.) -
        // degrade gracefully to "unavailable" rather than throwing from every evaluation call.
        return null;
      }
    }

    // Same contract as Evaluation.EvaluateStack/EvaluateProgram (assignable to pgp.Evaluate):
    // evaluates the program over every row of `data`, fills TrueResults/EstimatedResults, and
    // returns the task's configured score, or NaN to signal a rejected/invalid program.
    public static double EvaluateGPU(PgpAlgorithm pgp, RPN<Symbol> p, Task t, DataRecord data) {
      if (_gpuDisabled) return Evaluation.EvaluateStack(pgp, p, t, data);

      var ctx = _threadContext.Value;
      if (ctx == null) {
        return Evaluation.EvaluateStack(pgp, p, t, data); // graceful CPU fallback
      }

      int n = p.Count;
      var opcodes = new byte[n];
      var constants = new double[n];
      var varIndex = new int[n];
      int maxStackDepth = 0, currentDepth = 0;

      for (int i = 0; i < n; i++) {
        var sym = p[i];
        if (sym.Type == SymbolType.Constant) {
          opcodes[i] = OpConstant;
          constants[i] = sym.Con.Value;
          varIndex[i] = -1;
          currentDepth += 1;
        } else if (sym.Type == SymbolType.Variable) {
          opcodes[i] = OpVariable;
          constants[i] = sym.Var.Coefficient;
          varIndex[i] = sym.Var.Index;
          currentDepth += 1;
        } else {
          if (!_opcodeBySymbol.TryGetValue(sym.Opr.Symbol, out byte code)) {
            return Evaluation.EvaluateStack(pgp, p, t, data); // unsupported operator -> CPU fallback
          }
          opcodes[i] = code;
          varIndex[i] = -1;
          currentDepth -= sym.Opr.Arity - 1; // pops Arity operands, pushes 1 result
        }
        maxStackDepth = Math.Max(maxStackDepth, currentDepth);
      }

      if (currentDepth != 1) return double.NaN; // malformed program

      int rowCount = data.RowCount;
      double[]? estimates;

      // This thread owns `ctx` exclusively - no locking needed between CPU worker threads, each
      // has its own Accelerator/stream/cached buffer. A watchdog timeout guards against a stalled
      // OpenCL driver (observed on some integrated GPUs) hanging the whole run: if a single launch
      // does not complete in time, GPU evaluation is disabled process-wide from then on.
      try {
        estimates = RunKernelWithTimeout(ctx, data, opcodes, constants, varIndex, n, maxStackDepth, rowCount);
      } catch (TimeoutException) {
        _gpuDisabled = true;
        Console.Error.WriteLine(
          $"[EvaluationGpu] GPU kernel did not complete within {KernelTimeout.TotalSeconds:f0}s " +
          "(likely a stalled driver) - disabling GPU evaluation for the rest of this run, falling back to CPU.");
        return Evaluation.EvaluateStack(pgp, p, t, data);
      }

      if (estimates == null) return double.NaN;

      int targetIdx = t.VariableIndices[t.TargetVariable];

      for (int row = 0; row < rowCount; row++) {
        double result = estimates[row];
        if (double.IsNaN(result) || double.IsInfinity(result)) return double.NaN;
        p.TrueResults[row] = data.Data[targetIdx * rowCount + row];
        p.EstimatedResults[row] = result;
      }

      p.Score = t.Score.Compute(p);
      return p.Score;
    }

    // Uploads DataRecord.Data to this thread's device once and reuses it for every subsequent
    // call with the same DataRecord instance (identity check by reference - DataRecord is created
    // once per Fit run in Algorithm.cs and never mutated in place).
    private static ArrayView<double> GetOrUploadDataBuffer(ThreadGpuContext ctx, DataRecord data) {
      if (ctx.CachedDataRecordRef != data || ctx.CachedDataBuffer == null) {
        ctx.CachedDataBuffer?.Dispose();
        ctx.CachedDataBuffer = ctx.Accelerator.Allocate1D(data.Data);
        ctx.CachedDataRecordRef = data;
      }
      return ctx.CachedDataBuffer.View;
    }

    // Launches the kernel and blocks (on a background task) for at most KernelTimeout before
    // giving up, so a stalled OpenCL driver can never hang the calling (CPU population-parallel)
    // thread forever. Runs on this thread's own accelerator/context, so no cross-thread locking
    // is needed here.
    private static double[] RunKernelWithTimeout(ThreadGpuContext ctx, DataRecord data, byte[] opcodes, double[] constants, int[] varIndex, int n, int maxStackDepth, int rowCount) {
      var task = System.Threading.Tasks.Task.Run(() => {
        var dataView = GetOrUploadDataBuffer(ctx, data);

        using var opcodeBuffer = ctx.Accelerator.Allocate1D(opcodes);
        using var constantBuffer = ctx.Accelerator.Allocate1D(constants);
        using var varIndexBuffer = ctx.Accelerator.Allocate1D(varIndex);
        using var estimatesBuffer = ctx.Accelerator.Allocate1D<double>(rowCount);

        ctx.Kernel(rowCount, dataView, rowCount, opcodeBuffer.View, constantBuffer.View, varIndexBuffer.View, n, maxStackDepth, estimatesBuffer.View);
        ctx.Accelerator.Synchronize();

        return estimatesBuffer.GetAsArray1D();
      });

      if (!task.Wait(KernelTimeout)) throw new TimeoutException();
      return task.Result;
    }

    // GPU kernel body: one thread == one data row. Executes the flattened RPN program using a
    // small thread-local evaluation stack (ILGPU LocalMemory: private per-thread scratch space,
    // analogous to a stackalloc'd array in a CPU method).
    private static void EvaluateRowKernel(
      Index1D row,
      ArrayView<double> data, int rowCount,
      ArrayView<byte> opcodes,
      ArrayView<double> constants,
      ArrayView<int> varIndex,
      int programLength,
      int maxStackDepth,
      ArrayView<double> estimates) {

      var stack = LocalMemory.Allocate1D<double>(maxStackDepth <= 0 ? 1 : maxStackDepth);
      int sp = -1; // stack pointer; points at the current top element

      for (int i = 0; i < programLength; i++) {
        byte op = opcodes[i];

        if (op == OpConstant) {
          stack[++sp] = constants[i];
        } else if (op == OpVariable) {
          stack[++sp] = data[varIndex[i] * rowCount + row] * constants[i];
        } else if (op == OpAdd) {
          double b = stack[sp--]; double a = stack[sp--]; stack[++sp] = a + b;
        } else if (op == OpSub) {
          double b = stack[sp--]; double a = stack[sp--]; stack[++sp] = a - b;
        } else if (op == OpMul) {
          double b = stack[sp--]; double a = stack[sp--]; stack[++sp] = a * b;
        } else if (op == OpAnalyticQuotient) {
          double denom = stack[sp--]; double numer = stack[sp--];
          stack[++sp] = numer / XMath.Sqrt(1.0 + denom * denom);
        } else if (op == OpSin) {
          stack[sp] = XMath.Sin(stack[sp]);
        } else if (op == OpCos) {
          stack[sp] = XMath.Cos(stack[sp]);
        } else if (op == OpTan) {
          stack[sp] = XMath.Tan(stack[sp]);
        } else if (op == OpTanh) {
          stack[sp] = XMath.Tanh(stack[sp]);
        } else if (op == OpProtectedLog) {
          double v = stack[sp];
          stack[sp] = v > 0.0 ? XMath.Log(v) : 0.0;
        } else if (op == OpProtectedExp) {
          double v = stack[sp];
          stack[sp] = XMath.Exp(XMath.Min(XMath.Max(v, -100.0), 100.0));
        } else if (op == OpPi) {
          stack[sp] = stack[sp] * XMath.PI;
        }
      }

      double result = stack[sp];
      estimates[row] = double.IsNaN(result) || double.IsInfinity(result) ? double.NaN : result;
    }
  }
}
