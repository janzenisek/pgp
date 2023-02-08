namespace PGP.Utils {
  public class Misc {

    private readonly static string charset = "abcdefghijklmnopqrstuvwxyz0123456789";
    private static Random rnd = new Random();

    public static string GenerateId(int length) {
      string newId = "";

      for (int i = 0; i < length; i++) {
        newId += charset[rnd.Next(0, charset.Length)];
      }
      return newId;
    }

    public static string GetLastTopic(string topicStr) {
      var topics = topicStr.Split('/');

      return topics[topics.Length - 1];
    }

    // TODO: support # wildcard
    public static bool CompareTopics(string topicStr1, string topicStr2) {
      var topics1 = topicStr1.Split('/');
      var topics2 = topicStr2.Split('/');
      if (topics1.Length != topics2.Length) return false;

      for (int i = 0; i < topics1.Length; i++) {
        if (topics1[i] != topics2[i]) {
          if ((topics1[i] != "+" && topics2[i] != "+")
            && (topics1[i] != "*" && topics2[i] != "*")
            && (topics1[i] != "#" && topics2[i] != "#")) return false;
        }
      }
      return true;
    }

  }
}
