using System;
using System.Linq;
using SqlDsl.Utils;

namespace SqlDsl.UnitTests.FullPathTests.Environment
{
#pragma warning disable CS0660
#pragma warning disable CS0661
    public abstract class EqComparer
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
    
    public class Person : EqComparer
    {
        public long Id { get; set; }
        public string Name { get; set; }
        public Gender Gender { get; set; }
        public bool IsMember { get; set; }

        public Person() { }

        public Person(long id, string name, Gender gender, bool isMember)
        {
            Id = id;
            Name = name;
            Gender = gender;
            IsMember = isMember;
        }

        public override int GetHashCode() => $"{Id}.{Name}.{Gender}".GetHashCode();
        public override bool Equals(object p)
        {
            var person = p as Person;
            return person != null && 
                person.Id == Id && 
                person.Name == Name && 
                person.Gender == Gender && 
                person.IsMember == IsMember;
        }
        
        public override string ToString()
        {
            return $"{Id}: {Name}";
        }
    }
    
    public class PersonsData : EqComparer
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

    public enum Gender
    {
        Female = 1,
        Male = 2
    }
    
    public class PersonClass : EqComparer
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
    
    public class Class : EqComparer
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
    
    public class Tag : EqComparer
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
    
    public class ClassTag : EqComparer
    {
        public long ClassId { get; set; }
        public long TagId { get; set; }

        public override int GetHashCode() => $"{ClassId}.{TagId}".GetHashCode();
        public override bool Equals(object p)
        {
            var person = p as ClassTag;
            return person != null && person.ClassId == ClassId && person.TagId == TagId;
        }
        
        public override string ToString() => $"class: {ClassId}, tag: {TagId}";
    }
    
    public class Purchase : EqComparer
    {
        public long Id { get; set; }
        public float Amount { get; set; }
        public long PersonId { get; set; }
        public long PurchaedForPersonId { get; set; }
        public long? ClassId { get; set; }
        public DateTime DateUtc { get; set; }

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

    public class TableWithOneRowAndOneColumn : EqComparer
    {
        public int Value { get; set; }

        public override int GetHashCode() => Value.GetHashCode();
        public override bool Equals(object t)
        {
            var table = t as TableWithOneRowAndOneColumn;
            return table != null && 
                table.Value == Value;
        }
    }

    public class DataDump : EqComparer
    {
        public int Id { get; set; }
        public string Value { get; set; }

        public override int GetHashCode() => $"{Id}.{Value}".GetHashCode();
        public override bool Equals(object t)
        {
            var dd = t as DataDump;
            return dd != null && 
                dd.Id == Id && 
                dd.Value == Value;
        }
    }
}
