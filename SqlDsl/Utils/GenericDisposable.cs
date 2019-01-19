using System;

namespace SqlDsl.Utils
{
    /// <summary>
    /// Not thread safe
    /// </summary>
    class GenericDisposable : IDisposable
    {
        protected Action _dispose;

        public GenericDisposable(Action dispose)
        {
            _dispose = dispose ?? throw new ArgumentException(nameof(dispose));
        }

        public virtual void Dispose()
        {
            if (_dispose == null)
                return;

            _dispose();
            _dispose = null;
        }
    }

    /// <summary>
    /// A thread safe disposable that can be re-used
    /// </summary>
    class ReusableGenericDisposable : GenericDisposable
    {
        static readonly Action Nothing = () => {};

        [ThreadStatic]
        static ReusableGenericDisposable Instance = null;

        private ReusableGenericDisposable()
            : base(Nothing)
        {
            // reusable disposable is not valid until it is initialized
            _dispose = null;
        }

        public static IDisposable Build(Action dispose)
        {
            ReusableGenericDisposable instance = null;
            lock (Nothing)
            {
                instance = Instance;
                Instance = null;
            }

            if (instance == null)
                instance = new ReusableGenericDisposable();

            instance.Initialize(dispose);
            return instance;
        }

        private void Initialize(Action dispose)
        {
            _dispose = dispose ?? throw new ArgumentException(nameof(dispose));
        }

        public override void Dispose()
        {
            if (_dispose == null)
                throw new InvalidOperationException("Disposable has not been initialized");

            base.Dispose();

            lock (Nothing)
            {
                Instance = this;
            }
        }
    }
}