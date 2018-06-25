namespace Hangfire.PostgreSql.Entities
{
    public struct Option<T> where T : class
    {
        public T Value;
        public bool HasValue => Value != null;

        public Option(T value)
        {
            Value = value;
        }
    }
}
