namespace EKG.Common.Kafka.Internal;

internal static class ValueTaskTools
{
    public static ValueTask WhenAll(params ValueTask[] valueTasks)
    {
        if (valueTasks.Length == 0) return new ValueTask();

        var toAwait = new Task[valueTasks.Length];
        var completedTasks = 0;

        for (var i = 0; i < valueTasks.Length; i++)
        {
            if (!valueTasks[i].IsCompletedSuccessfully)
                toAwait[i] = valueTasks[i].AsTask();
            else
                completedTasks++;
        }

        return completedTasks == valueTasks.Length ? new ValueTask() : new ValueTask(Task.WhenAll(toAwait));
    }
}
