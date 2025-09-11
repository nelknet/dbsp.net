namespace DBSP.Operators.Temporal

open System.Threading.Tasks
open DBSP.Storage

module TemporalOperators =
    /// Materialize a snapshot at a given time from a temporal trace.
    let snapshotAt (trace: ITemporalTrace<'K,'V>) (time: int64) : Task<struct('K*'V*int64) array> =
        task {
            let! seq = trace.QueryAtTime(time)
            return seq |> Seq.toArray
        }

    /// Materialize a time range [startTime, endTime] as per-time batches.
    let range (trace: ITemporalTrace<'K,'V>) (startTime: int64) (endTime: int64) : Task<struct(int64 * struct('K*'V*int64) array) array> =
        task {
            let! seq = trace.QueryTimeRange(startTime, endTime)
            return seq |> Seq.toArray
        }

