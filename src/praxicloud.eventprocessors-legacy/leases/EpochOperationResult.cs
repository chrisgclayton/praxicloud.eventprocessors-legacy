using System;
using System.Collections.Generic;
using System.Text;

namespace praxicloud.eventprocessors.legacy.leases
{
    public enum EpochOperationResult
    {
        Unknown = 0,
        Success = 1,
        Failure = 2,
        Conflict = 3
    }
}
