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
        public Gender Gender { get; set; }

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

    enum Gender
    {
        Female = 1,
        Male = 2
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
    
    class Purchase : EqComparer
    {
        public int Id { get; set; }
        public float Amount { get; set; }
        public int PersonId { get; set; }
        public int PurchaedForPersonId { get; set; }
        public int? ClassId { get; set; }

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
    
    class DataTypeTest
    {
        public byte Byte;
        public sbyte SByte;
        public bool Bool;
        public byte? Byte_N;
        public sbyte? SByte_N;
        public bool? Bool_N;

        public short Short;
        public int Int;
        public long Long;
        public short? Short_N;
        public int? Int_N;
        public long? Long_N;

        public ushort UShort;
        public uint UInt;
        public ulong ULong;
        public ushort? UShort_N;
        public uint? UInt_N;
        public ulong? ULong_N;

        public float Float;
        public double Double;
        public decimal Decimal;
        public float? Float_N;
        public double? Double_N;
        public decimal? Decimal_N;
                
        public char Char;
        public DateTime DateTime;
        public string String;
        public char? Char_N;
        public DateTime? DateTime_N;

        public Guid Guid;
        public Guid? Guid_N;

        public IEnumerable<char> CharEnumerable;
        public List<char> CharList;
        public char[] CharArray;

        public IEnumerable<byte> ByteEnumerable;
        public List<byte> ByteList;
        public byte[] ByteArray;

        public TestEnum TestEnum;
        public TestEnum? TestEnum_N;
    }

    public enum TestEnum
    {
        Option1 = 1,
        Option2 = 2
    }
}
