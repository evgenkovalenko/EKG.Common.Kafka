namespace EKG.Common.Kafka.Internal;

using System.Text.RegularExpressions;

internal static class NamingTools
{
    private static readonly Regex EnvRegex = new(@"\[Env:(\w+)\]", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    public static string FillInVariableValues(string value)
    {
        var newValue = value
            .Replace("[HostName]", Environment.MachineName)
            .Replace("[Guid]", Guid.NewGuid().ToString("n")[..10]);

        return EnvRegex.Replace(newValue, m =>
            Environment.GetEnvironmentVariable(m.Groups[1].Value) ?? "NotFound");
    }
}
