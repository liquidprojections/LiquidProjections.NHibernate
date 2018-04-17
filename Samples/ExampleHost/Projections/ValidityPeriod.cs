using System;

namespace LiquidProjections.ExampleHost.Projections
{
    public class ValidityPeriod : IPersistable
    {
        public ValidityPeriod(DateTime? startDateTime, DateTime? endDateTime)
        {
            From = startDateTime;
            To = endDateTime;
        }

        public ValidityPeriod()
        {
        }

        public int Sequence { get; set; }
        public DateTime? From { get; set; }
        public DateTime? To { get; set; }
        public string Status { get; set; }

        protected bool Equals(ValidityPeriod other)
        {
            return Sequence == other.Sequence && From.Equals(other.From) && To.Equals(other.To) && string.Equals(Status, other.Status, StringComparison.InvariantCulture);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            var other = obj as ValidityPeriod;
            return other != null && Equals(other);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Sequence;
                hashCode = (hashCode * 397) ^ From.GetHashCode();
                hashCode = (hashCode * 397) ^ To.GetHashCode();
                hashCode = (hashCode * 397) ^ (Status != null ? StringComparer.InvariantCulture.GetHashCode(Status) : 0);
                return hashCode;
            }
        }
    }
}