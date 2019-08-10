using NUnit.Framework;
using SqlDsl.Utils;

namespace SqlDsl.UnitTests.FullPathTests
{
    [TestFixture]
    public class DubugFlagTests
    {
        [Test]
        public void Ensure_DebugConvert_IsFalse()
        {
            // arrange
            // act
            // assert
            Assert.False(ReflectionUtils.DebugConvert);
        }
    }
}