using NUnit.Framework;
using SqlDsl.ObjectBuilders;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.UnitTests.ObjectBuilders
{
    [TestFixture]
    public class Builders_1LevelDeepTests
    {
        class SimpleProperties 
        {
            public string Property1 { get; set; } 
            public int Property2 { get; set; } 
        }

        public static Action<object, IEnumerable, ILogger> NullAction = null;

        [Test]
        public void Build_WithNullInput_BuildsCorrectly()
        {
            // arrange
            // act
            var result = Builders.GetBuilder<SimpleProperties>().Build(null, null);

            // assert
            Assert.NotNull(result);
        }

        [Test]
        public void Build_WithSimpleProperties_BuildsCorrectly()
        {
            // arrange
            // act
            var result = Builders.GetBuilder<SimpleProperties>().Build(new ObjectGraph
            {
                SimpleProps = new [] 
                {
                    ("Property1", new object[] { "Hello" }.Skip(0), false),
                    ("Property2", new object[] { 7 }.Skip(0), false)
                }
            }, null);

            // assert
            Assert.AreEqual("Hello", result.Property1);
            Assert.AreEqual(7, result.Property2);
        }

        [Test]
        public void Build_ExpectingIntGetsNull_ThrowsError()
        {
            // arrange
            // act
            // assert
            Assert.Throws(
                typeof(InvalidOperationException), 
                () => Builders.GetBuilder<SimpleProperties>().Build(new ObjectGraph
                {
                    SimpleProps = new [] 
                    {
                        ("Property2", new object[] { null }.Skip(0), false)
                    }
                }, null));
        }

        [Test]
        public void Build_WithExpectingIntGetsDate_ThrowsError()
        {
            // arrange
            // act
            // assert
            Assert.Throws(
                typeof(InvalidCastException), 
                () => Builders.GetBuilder<SimpleProperties>().Build(new ObjectGraph
                {
                    SimpleProps = new [] 
                    {
                        ("Property2", new object[] { DateTime.Now }.Skip(0), false)
                    }
                }, null));
        }

        [Test]
        public void Build_WithSimpleProperties_ThrowsErrorWhenMultipleValues()
        {
            // arrange
            // act
            // assert
            Assert.Throws(
                typeof(InvalidOperationException), 
                () => Builders.GetBuilder<SimpleProperties>().Build(new ObjectGraph
                {
                    SimpleProps = new [] 
                    {
                        ("Property1", new object[] { "Hello", "Hello again" }.Skip(0), false),
                        ("Property2", new object[] { 7, 8 }.Skip(0), false)
                    }
                }, null));
        }

        class SimplePropertiesChild : SimpleProperties{}

        [Test]
        public void Build_ParentHasProperties_BuildsCorrectly()
        {
            // arrange
            // act
            var result = Builders.GetBuilder<SimplePropertiesChild>().Build(new ObjectGraph
            {
                SimpleProps = new [] 
                {
                    ("Property1", new object[] { "Hello" }.Skip(0), false),
                    ("Property2", new object[] { 7 }.Skip(0), false)
                }
            }, null);

            // assert
            Assert.AreEqual("Hello", result.Property1);
            Assert.AreEqual(7, result.Property2);
        }
        
        class MultipleSimpleProperties 
        {
            public IEnumerable<string> Property1 { get; set; } 
            public IEnumerable<int> Property2 { get; set; } 
        }

        [Test]
        public void Build_WithMultipleSimpleProperties_BuildsCorrectly()
        {
            // arrange
            // act
            var result = Builders.GetBuilder<MultipleSimpleProperties>().Build(new ObjectGraph
            {
                SimpleProps = new [] 
                {
                    ("Property1", new object[] { "Hello", "Hello again" }.Skip(0), false),
                    ("Property2", new object[] { 7, 8 }.Skip(0), false)
                }
            }, null);

            // assert
            Assert.AreEqual(2, result.Property1.Count());
            Assert.AreEqual("Hello", result.Property1.ElementAt(0));
            Assert.AreEqual("Hello again", result.Property1.ElementAt(1));
            
            Assert.AreEqual(2, result.Property2.Count());
            Assert.AreEqual(7, result.Property2.ElementAt(0));
            Assert.AreEqual(8, result.Property2.ElementAt(1));
        }

        [Test]
        public void Build_WithMultipleSimpleProperties_CreatesEmptyLists()
        {
            // arrange
            // act
            var result = Builders.GetBuilder<MultipleSimpleProperties>().Build(null, null);

            // assert
            Assert.NotNull(result.Property1);
            Assert.NotNull(result.Property2);
        }

        static ObjectGraph CollectionTestValues() => new ObjectGraph
        {
            SimpleProps = new [] { ("Property1", new object[] { "Hello", "Hello again" }.Skip(0), false) }
        };
            
        static void AssertCollectionTest(IEnumerable<string> values)
        {
            Assert.NotNull(values);
            Assert.AreEqual(2, values.Count());
            Assert.AreEqual("Hello", values.ElementAt(0));
            Assert.AreEqual("Hello again", values.ElementAt(1));
        }

        interface ITheCollection { IEnumerable<string> Property1 { get; } }
        class WithICollection : ITheCollection
        { 
            public ICollection<string> Property1 { get; set; }
            IEnumerable<string> ITheCollection.Property1 => Property1;
        }
        class WithIList : ITheCollection
        { 
            public IList<string> Property1 { get; set; } 
            IEnumerable<string> ITheCollection.Property1 => Property1;
        }
        class WithList  : ITheCollection
        { 
            public List<string> Property1 { get; set; } 
            IEnumerable<string> ITheCollection.Property1 => Property1;
        }
        class WithIEnumerable  : ITheCollection
        { 
            public IEnumerable<string> Property1 { get; set; }
            IEnumerable<string> ITheCollection.Property1 => Property1;
        }
        class WithIReadOnlyCollection  : ITheCollection
        { 
            public IReadOnlyCollection<string> Property1 { get; set; }
            IEnumerable<string> ITheCollection.Property1 => Property1;
        }
        class WithIReadOnlyList  : ITheCollection
        { 
            public IReadOnlyList<string> Property1 { get; set; }
            IEnumerable<string> ITheCollection.Property1 => Property1;
        }
        class WithHashSet  : ITheCollection
        { 
            public HashSet<string> Property1 { get; set; }
            IEnumerable<string> ITheCollection.Property1 => Property1;
        }
        class WithArray  : ITheCollection
        { 
            public string[] Property1 { get; set; }
            IEnumerable<string> ITheCollection.Property1 => Property1;
        }

        [Test]
        [TestCase(typeof(WithICollection))]
        [TestCase(typeof(WithIList))]
        [TestCase(typeof(WithList))]
        [TestCase(typeof(WithIEnumerable))]
        [TestCase(typeof(WithIReadOnlyCollection))]
        [TestCase(typeof(WithIReadOnlyList))]
        [TestCase(typeof(WithHashSet))]
        [TestCase(typeof(WithArray))]
        public void Build_WithAllSuportedCollectionTypes_BuildsCorrectly(Type testType)
        {
            // arrange
            // act
            var result = (ITheCollection)Builders.GetBuilder(testType).Build(
                CollectionTestValues(), null);

            // assert
            AssertCollectionTest(result.Property1);
        }

        [Test]
        [TestCase(typeof(WithICollection))]
        [TestCase(typeof(WithIList))]
        [TestCase(typeof(WithList))]
        [TestCase(typeof(WithIEnumerable))]
        [TestCase(typeof(WithIReadOnlyCollection))]
        [TestCase(typeof(WithIReadOnlyList))]
        [TestCase(typeof(WithHashSet))]
        [TestCase(typeof(WithArray))]
        public void Build_WithICollection_CreatesEmptyCollection(Type testType)
        {
            // arrange
            // act
            var result = (ITheCollection)Builders.GetBuilder(testType).Build( 
                null, null);

            // assert
            Assert.NotNull(result.Property1);
            Assert.IsEmpty(result.Property1);
        }
    }
}