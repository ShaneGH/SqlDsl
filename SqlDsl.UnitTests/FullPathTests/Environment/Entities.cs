using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using SqlDsl.Utils;

namespace SqlDsl.UnitTests.FullPathTests.Environment
{
#pragma warning disable CS0660
#pragma warning disable CS0661
    abstract class EqComparer
#pragma warning restore CS0661
#pragma warning restore CS0660
    {        
        public static bool operator !=(EqComparer a, EqComparer b) => !(a == b);

        public static bool operator ==(EqComparer a, EqComparer b)
        {
            return Object.ReferenceEquals(null, a) ?
                Object.ReferenceEquals(null, b) :
                a.Equals(b);
        }
    }
    
    class Person : EqComparer
    {
        public int Id { get; set; }
        public string Name { get; set; }

        public override int GetHashCode() => $"{Id}.{Name}".GetHashCode();
        public override bool Equals(object p)
        {
            var person = p as Person;
            return person != null && person.Id == Id && person.Name == Name;
        }
        
        public override string ToString()
        {
            return $"{Id}: {Name}";
        }
    }
    
    class PersonClass : EqComparer
    {
        public int PersonId { get; set; }
        public int ClassId { get; set; }

        public override int GetHashCode() => $"{PersonId}.{ClassId}".GetHashCode();
        public override bool Equals(object p)
        {
            var person = p as PersonClass;
            return person != null && person.PersonId == PersonId && person.ClassId == ClassId;
        }
    }
    
    class Class : EqComparer
    {
        public int Id { get; set; }
        public string Name { get; set; }

        public override int GetHashCode() => $"{Id}.{Name}".GetHashCode();
        public override bool Equals(object p)
        {
            var person = p as Class;
            return person != null && person.Id == Id && person.Name == Name;
        }
    }
    
    class Tag : EqComparer
    {
        public int Id { get; set; }
        public string Name { get; set; }

        public override int GetHashCode() => $"{Id}.{Name}".GetHashCode();
        public override bool Equals(object p)
        {
            var person = p as Tag;
            return person != null && person.Id == Id && person.Name == Name;
        }
    }
    
    class ClassTag : EqComparer
    {
        public int ClassId { get; set; }
        public int TagId { get; set; }

        public override int GetHashCode() => $"{ClassId}.{TagId}".GetHashCode();
        public override bool Equals(object p)
        {
            var person = p as ClassTag;
            return person != null && person.ClassId == ClassId && person.TagId == TagId;
        }
    }
}
