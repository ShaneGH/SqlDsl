using NUnit.Framework;
using SqlDsl.ObjectBuilders;
using System;
using System.Linq;

namespace SqlDsl.UnitTests.ObjectBuilders
{
    [TestFixture]
    public class Builders_2LevelsDeepTests
    {
        class Level1 
        {
            public string Property1 { get; set; } 
            public int Property2 { get; set; } 
        }

        class Level2
        {
            public Level1 Level1Properties {get; set; }
        }

        [Test]
        public void Build_WithNullInput_BuildsCorrectly()
        {
            // arrange
            // act
            var result = Builders.GetBuilder<Level2>().Build(null, null);

            // assert
            Assert.NotNull(result);
            Assert.Null(result.Level1Properties);
        }

        [Test]
        public void Build_WithComplexProperties_BuildsCorrectly()
        {
            // arrange
            // act
            var result = Builders.GetBuilder<Level2>().Build(new TestObjectGraph()
            {
                ComplexProps = new []
                {
                    (nameof(Level2.Level1Properties), new []
                    {
                        (ReusableObjectGraph)new TestObjectGraph()
                        {
                            SimpleProps = new [] 
                            {
                                ("Property1", new object[] { "Hello" }.Skip(0), false),
                                ("Property2", new object[] { 7 }.Skip(0), false)
                            }.Skip(0)
                        }
                    }.Skip(0))
                }
            }, null);

            // assert
            Assert.NotNull(result);
            Assert.NotNull(result.Level1Properties);
            Assert.AreEqual("Hello", result.Level1Properties.Property1);
            Assert.AreEqual(7, result.Level1Properties.Property2);
        }

        [Test]
        public void Build_WithComplexProperties_ThrowsErrorWhenMultipleValues()
        {
            // arrange
            // act
            // assert
            Assert.Throws(typeof(InvalidOperationException), () => Builders.GetBuilder<Level2>().Build(new TestObjectGraph()
            {
                ComplexProps = new []
                {
                    (nameof(Level2.Level1Properties), new []
                    {
                        (ReusableObjectGraph)new TestObjectGraph()
                        {
                            SimpleProps = new [] 
                            {
                                ("Property1", new object[] { "Hello" }.Skip(0), false),
                                ("Property2", new object[] { 7 }.Skip(0), false)
                            }.Skip(0)
                        },
                        new TestObjectGraph()
                        {
                            SimpleProps = new [] 
                            {
                                ("Property1", new object[] { "Hello" }.Skip(0), false),
                                ("Property2", new object[] { 7 }.Skip(0), false)
                            }.Skip(0)
                        }
                    }.Skip(0))
                }
            }, null));
        }

        class Level1Multiple
        {
            public Level1[] SimpleProperties {get; set; }
        }

        [Test]
        public void Build_WithMultipleComplexProperties_BuildsCorrectly()
        {
            // arrange
            // act
            var result = Builders.GetBuilder<Level1Multiple>().Build(new TestObjectGraph()
            {
                ComplexProps = new []
                {
                    (nameof(Level1Multiple.SimpleProperties), new []
                    {
                        (ReusableObjectGraph)new TestObjectGraph()
                        {
                            SimpleProps = new [] 
                            {
                                ("Property1", new object[] { "Hello" }.Skip(0), false),
                                ("Property2", new object[] { 7 }.Skip(0), false)
                            }.Skip(0)
                        },
                        new TestObjectGraph()
                        {
                            SimpleProps = new [] 
                            {
                                ("Property1", new object[] { "Goodbye" }.Skip(0), false),
                                ("Property2", new object[] { 8 }.Skip(0), false)
                            }.Skip(0)
                        }
                    }.Skip(0))
                }
            }, null);

            // assert
            Assert.NotNull(result);
            Assert.NotNull(result.SimpleProperties);
            Assert.AreEqual(2, result.SimpleProperties.Count());
            Assert.AreEqual("Hello", result.SimpleProperties.ElementAt(0).Property1);
            Assert.AreEqual(7, result.SimpleProperties.ElementAt(0).Property2);
            Assert.AreEqual("Goodbye", result.SimpleProperties.ElementAt(1).Property1);
            Assert.AreEqual(8, result.SimpleProperties.ElementAt(1).Property2);
        }

        [Test]
        public void Build_WithMultipleComplexProperties_CreatesEmptyList()
        {
            // arrange
            // act
            var result = Builders.GetBuilder<Level1Multiple>().Build(null, null);

            // assert
            Assert.NotNull(result);
            Assert.NotNull(result.SimpleProperties);
            Assert.AreEqual(0, result.SimpleProperties.Count());
        }
    }
}