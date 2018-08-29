using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using SqlDsl.ObjectBuilders;

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
            var result = (Level2)Builders.Build(typeof(Level2), null);

            // assert
            Assert.NotNull(result);
            Assert.Null(result.Level1Properties);
        }

        [Test]
        public void Build_WithComplexProperties_BuildsCorrectly()
        {
            // arrange
            // act
            var result = (Level2)Builders.Build(typeof(Level2), new ObjectGraph
            {
                ComplexProps = new []
                {
                    (nameof(Level2.Level1Properties), new []
                    {
                        new ObjectGraph
                        {
                            SimpleProps = new [] 
                            {
                                ("Property1", new object[] { "Hello" }.Skip(0)),
                                ("Property2", new object[] { 7 }.Skip(0))
                            }.Skip(0)
                        }
                    }.Skip(0))
                }
            });

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
            Assert.Throws(typeof(InvalidOperationException), () => Builders.Build(typeof(Level2), new ObjectGraph
            {
                ComplexProps = new []
                {
                    (nameof(Level2.Level1Properties), new []
                    {
                        new ObjectGraph
                        {
                            SimpleProps = new [] 
                            {
                                ("Property1", new object[] { "Hello" }.Skip(0)),
                                ("Property2", new object[] { 7 }.Skip(0))
                            }.Skip(0)
                        },
                        new ObjectGraph
                        {
                            SimpleProps = new [] 
                            {
                                ("Property1", new object[] { "Hello" }.Skip(0)),
                                ("Property2", new object[] { 7 }.Skip(0))
                            }.Skip(0)
                        }
                    }.Skip(0))
                }
            }));
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
            var result = (Level1Multiple)Builders.Build(typeof(Level1Multiple), new ObjectGraph
            {
                ComplexProps = new []
                {
                    (nameof(Level1Multiple.SimpleProperties), new []
                    {
                        new ObjectGraph
                        {
                            SimpleProps = new [] 
                            {
                                ("Property1", new object[] { "Hello" }.Skip(0)),
                                ("Property2", new object[] { 7 }.Skip(0))
                            }.Skip(0)
                        },
                        new ObjectGraph
                        {
                            SimpleProps = new [] 
                            {
                                ("Property1", new object[] { "Goodbye" }.Skip(0)),
                                ("Property2", new object[] { 8 }.Skip(0))
                            }.Skip(0)
                        }
                    }.Skip(0))
                }
            });

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
            var result = (Level1Multiple)Builders.Build(typeof(Level1Multiple), null);

            // assert
            Assert.NotNull(result);
            Assert.NotNull(result.SimpleProperties);
            Assert.AreEqual(0, result.SimpleProperties.Count());
        }
    }
}