using System;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests
{
    [SqlTestAttribute(SqlType.Sqlite)]
    public class ExecutionMethodTests : FullPathTestBase
    {
        public ExecutionMethodTests(SqlType testFlavour)
            : base(testFlavour)
        {
        }
        
        [Test]
        public void ToIEnumerable_WithMultiple_ReturnsCorrectData()
        {
            // arrange
            // act
            var data = Query<Person>()
                .ToIEnumerable(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(Data.People, data);
        }
        
        [Test]
        public async Task ToIEnumerableAsync_WithMultiple_ReturnsCorrectData()
        {
            // arrange
            // act
            var data = await Query<Person>()
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(Data.People, data);
        }
        
        [Test]
        public void ToList_WithMultiple_ReturnsCorrectData()
        {
            // arrange
            // act
            var data = Query<Person>()
                .ToList(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(Data.People, data);
        }
        
        [Test]
        public async Task ToListAsync_WithMultiple_ReturnsCorrectData()
        {
            // arrange
            // act
            var data = await Query<Person>()
                .ToListAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(Data.People, data);
        }
        
        [Test]
        public void ToArray_WithMultiple_ReturnsCorrectData()
        {
            // arrange
            // act
            var data = Query<Person>()
                .ToArray(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(Data.People, data);
        }
        
        [Test]
        public async Task ToArrayAsync_WithMultiple_ReturnsCorrectData()
        {
            // arrange
            // act
            var data = await Query<Person>()
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(Data.People, data);
        }
        
        [Test]
        public void First_WithMultiple_ReturnsCorrectData()
        {
            // arrange
            // act
            var data = Query<Person>()
                .OrderBy(x => x.Id)
                .First(Executor, logger: Logger);

            // assert
            Assert.AreEqual(Data.People.John, data);
        }
        
        [Test]
        public async Task FirstAsync_WithMultiple_ReturnsCorrectData()
        {
            // arrange
            // act
            var data = await Query<Person>()
                .OrderBy(x => x.Id)
                .FirstAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(Data.People.John, data);
        }
        
        [Test]
        public void First_WithNone_ThrowsException()
        {
            // arrange
            // act
            // assert
            Assert.Throws(typeof(InvalidOperationException), () => 
                Query<Person>()
                    .Where(x => x.Id == 999)
                    .First(Executor, logger: Logger));
        }
        
        [Test]
        public void FirstAsync_WithNone_ThrowsException()
        {
            // arrange
            // act
            // assert
            Assert.ThrowsAsync(typeof(InvalidOperationException), () => 
                Query<Person>()
                    .Where(x => x.Id == 999)
                    .FirstAsync(Executor, logger: Logger));
        }
        
        [Test]
        public void FirstOrDefault_WithMultiple_ReturnsCorrectData()
        {
            // arrange
            // act
            var data = Query<Person>()
                .OrderBy(x => x.Id)
                .FirstOrDefault(Executor, logger: Logger);

            // assert
            Assert.AreEqual(Data.People.John, data);
        }
        
        [Test]
        public async Task FirstOrDefaultAsync_WithMultiple_ReturnsCorrectData()
        {
            // arrange
            // act
            var data = await Query<Person>()
                .OrderBy(x => x.Id)
                .FirstOrDefaultAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(Data.People.John, data);
        }
        
        [Test]
        public void FirstOrDefault_WithNone_ReturnsNull()
        {
            // arrange
            // act
            var result = Query<Person>()
                .Where(x => x.Id == 999)
                .FirstOrDefault(Executor, logger: Logger);

            // assert
            Assert.IsNull(result);
        }
        
        [Test]
        public async Task FirstOrDefaultAsync_WithNone_ReturnsNull()
        {
            // arrange
            // act
            var result = await Query<Person>()
                .Where(x => x.Id == 999)
                .FirstOrDefaultAsync(Executor, logger: Logger);

            // assert
            Assert.IsNull(result);
        }
        
        [Test]
        public void Single_WithMultiple_ThrowsException()
        {
            // arrange
            // act
            // assert
            Assert.Throws(typeof(InvalidOperationException), () =>
                Query<Person>()
                    .OrderBy(x => x.Id)
                    .Single(Executor, logger: Logger));
        }
        
        [Test]
        public void SingleAsync_WithMultiple_ThrowsException()
        {
            // arrange
            // act
            // assert
            Assert.ThrowsAsync(typeof(InvalidOperationException), () =>
                Query<Person>()
                    .OrderBy(x => x.Id)
                    .SingleAsync(Executor, logger: Logger));
        }
        
        [Test]
        public void Single_WithOne_ReturnsCorrectData()
        {
            // arrange
            // act
            var data = Query<Person>()
                .Where(x => x.Id == Data.People.John.Id)
                .Single(Executor, logger: Logger);

            // assert
            Assert.AreEqual(Data.People.John, data);
        }
        
        [Test]
        public async Task SingleAsync_WithOne_ReturnsCorrectData()
        {
            // arrange
            // act
            var data = await Query<Person>()
                .Where(x => x.Id == Data.People.John.Id)
                .SingleAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(Data.People.John, data);
        }
        
        [Test]
        public void Single_WithNone_ThrowsException()
        {
            // arrange
            // act
            // assert
            Assert.Throws(typeof(InvalidOperationException), () => 
                Query<Person>()
                    .Where(x => x.Id == 999)
                    .Single(Executor, logger: Logger));
        }
        
        [Test]
        public void SingleAsync_WithNone_ThrowsException()
        {
            // arrange
            // act
            // assert
            Assert.ThrowsAsync(typeof(InvalidOperationException), () => 
                Query<Person>()
                    .Where(x => x.Id == 999)
                    .SingleAsync(Executor, logger: Logger));
        }
        
        [Test]
        public void SingleOrDefault_WithMultiple_ThrowsException()
        {
            // arrange
            // act
            // assert
            Assert.Throws(typeof(InvalidOperationException), () => 
                Query<Person>()
                    .OrderBy(x => x.Id)
                    .SingleOrDefault(Executor, logger: Logger));
        }
        
        [Test]
        public void SingleOrDefaultAsync_WithMultiple_ThrowsException()
        {
            // arrange
            // act
            // assert
            Assert.ThrowsAsync(typeof(InvalidOperationException), () => 
                Query<Person>()
                    .OrderBy(x => x.Id)
                    .SingleOrDefaultAsync(Executor, logger: Logger));
        }
        
        [Test]
        public void SingleOrDefault_WithOne_ReturnsCorrectData()
        {
            // arrange
            // act
            var data = Query<Person>()
                .Where(x => x.Id == Data.People.John.Id)
                .SingleOrDefault(Executor, logger: Logger);

            // assert
            Assert.AreEqual(Data.People.John, data);
        }
        
        [Test]
        public async Task SingleOrDefaultAsync_WithOne_ReturnsCorrectData()
        {
            // arrange
            // act
            var data = await Query<Person>()
                .Where(x => x.Id == Data.People.John.Id)
                .SingleOrDefaultAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(Data.People.John, data);
        }
        
        [Test]
        public void SingleOrDefault_WithNone_ReturnsNull()
        {
            // arrange
            // act
            var result = Query<Person>()
                .Where(x => x.Id == 999)
                .SingleOrDefault(Executor, logger: Logger);

            // assert
            Assert.IsNull(result);
        }
        
        [Test]
        public async Task SingleOrDefaultAsync_WithNone_ReturnsNull()
        {
            // arrange
            // act
            var result = await Query<Person>()
                .Where(x => x.Id == 999)
                .SingleOrDefaultAsync(Executor, logger: Logger);

            // assert
            Assert.IsNull(result);
        }
    }
}