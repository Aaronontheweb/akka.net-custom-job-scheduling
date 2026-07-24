namespace Akka.CustomerJobScheduling.Core.Jobs;

/// <summary>
/// Describes the total size of the job in terms of "units of execution"
///
/// i.e. how many rows in a table need to be ETL'd, how many lines
/// in a spreadsheet need to be imported, etc
/// </summary>
/// <param name="Size">Underlying value.</param>
public readonly record struct JobSize(uint Size)
{
    public static readonly JobSize Unknown = new(0);
}