using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;

using SqlDsl.ObjectBuilders;

namespace SqlDsl.UnitTests.ObjectBuilders
{
    [TestFixture]
    public class Builders_3LevelsDeepTests
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

        class Level3
        {
            public Level2 Level2Properties {get; set; }
        }

        [Test]
        public void Build_WithComplexProperties_BuildsCorrectly()
        {
            // arrange
            // act
            var result = (Level3)Builders.Build(typeof(Level3), new ObjectGraph
            {
                ComplexProps = new []
                {
                    (nameof(Level3.Level2Properties), new []
                    {
                        new ObjectGraph
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
                        }
                    }.Skip(0))
                }
            });

            // assert
            Assert.NotNull(result);
            Assert.NotNull(result.Level2Properties);
            Assert.NotNull(result.Level2Properties.Level1Properties);
            Assert.AreEqual("Hello", result.Level2Properties.Level1Properties.Property1);
            Assert.AreEqual(7, result.Level2Properties.Level1Properties.Property2);
        }
    }
}