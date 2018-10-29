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
        public long Id { get; set; }
        public string Name { get; set; }
        public Gender Gender { get; set; }

        public Person() { }

        public Person(long id, string name, Gender gender)
        {
            Id = id;
            Name = name;
            Gender = gender;
        }

        public override int GetHashCode() => $"{Id}.{Name}.{Gender}".GetHashCode();
        public override bool Equals(object p)
        {
            var person = p as Person;
            return person != null && person.Id == Id && person.Name == Name && person.Gender == Gender;
        }
        
        public override string ToString()
        {
            return $"{Id}: {Name}";
        }
    }
    
    class PersonsData : EqComparer
    {
        public long PersonId { get; set; }
        public byte[] Data { get; set; }

        public override int GetHashCode() => $"{PersonId}.{Data.Select(b => (int)b).JoinString(",")}".GetHashCode();
        public override bool Equals(object p)
        {
            var person = p as PersonsData;
            if (person == null || person.PersonId != PersonId)
                return false;

            if (Data == person.Data)
                return true;

            if (Data == null || person.Data == null)
                return false;

            if (Data.Length != person.Data.Length)
                return false;

            return Data
                .Zip(person.Data, (x, y) => (x: x, y: y))
                .All(x => x.x == x.y);
        }
        
        public override string ToString()
        {
            return $"{PersonId}: Some data";
        }
    }

    enum Gender
    {
        Female = 1,
        Male = 2
    }
    
    class PersonClass : EqComparer
    {
        public long PersonId { get; set; }
        public long ClassId { get; set; }

        public override int GetHashCode() => $"{PersonId}.{ClassId}".GetHashCode();
        public override bool Equals(object p)
        {
            var person = p as PersonClass;
            return person != null && person.PersonId == PersonId && person.ClassId == ClassId;
        }
    }
    
    class Class : EqComparer
    {
        public long Id { get; set; }
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
        public long Id { get; set; }
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
        public long ClassId { get; set; }
        public long TagId { get; set; }

        public override int GetHashCode() => $"{ClassId}.{TagId}".GetHashCode();
        public override bool Equals(object p)
        {
            var person = p as ClassTag;
            return person != null && person.ClassId == ClassId && person.TagId == TagId;
        }
    }
    
    class Purchase : EqComparer
    {
        public long Id { get; set; }
        public float Amount { get; set; }
        public long PersonId { get; set; }
        public long PurchaedForPersonId { get; set; }
        public long? ClassId { get; set; }

        public override int GetHashCode() => $"{Id}.{Amount}.{PersonId}.{ClassId}.{PurchaedForPersonId}".GetHashCode();
        public override bool Equals(object p)
        {
            var person = p as Purchase;
            return person != null && 
                person.Id == Id && 
                person.Amount == Amount && 
                person.PersonId == PersonId &&
                person.PurchaedForPersonId == PurchaedForPersonId &&
                person.ClassId == ClassId;
        }
    }
}
