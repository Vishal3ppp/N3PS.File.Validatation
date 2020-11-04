using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace N3PS.File.Compare.BusinessLogic
{


    [Table("ICATable2")]
    public class DynamicClass2 : DynamicObject
    {
        public Dictionary<string, object> _fields;
        public DynamicClass2()
        {
        }
            public DynamicClass2(List<Field> fields)
        {
            _fields = new Dictionary<string, object>();
            fields.ForEach(x => _fields.Add(x.Name,
                null));
        }

        public override bool TrySetMember(SetMemberBinder binder, object value)
        {
            if (_fields.ContainsKey(binder.Name))
            {
                _fields[binder.Name] = value;
                //if (value.GetType() == type)
                //{
                //    _fields[binder.Name] = new KeyValuePair<Type, object>(type, value);
                //    return true;
                //}
                //else throw new Exception("Value " + value + " is not of type " + type.Name);
            }
            return false;
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            result = _fields[binder.Name];
            return true;
        }
    }



    [Table("ICATable1")]
    public  class DynamicClass1 : DynamicObject
    {
        public Dictionary<string, object> _fields;

        public DynamicClass1()
        { }
            public DynamicClass1(List<Field> fields)
        {
            _fields = new Dictionary<string, object>();
            fields.ForEach(x => _fields.Add(x.Name,
                null));
        }

        public override bool TrySetMember(SetMemberBinder binder, object value)
        {
            if (_fields.ContainsKey(binder.Name))
            {
                _fields[binder.Name] = value;
                //if (value.GetType() == type)
                //{
                //    _fields[binder.Name] = new KeyValuePair<Type, object>(type, value);
                //    return true;
                //}
                //else throw new Exception("Value " + value + " is not of type " + type.Name);
            }
            return false;
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            result = _fields[binder.Name];
            return true;
        }
    }


    [Table("ICADeletedTable1")]
    public class ICADeletedTable1 : DynamicObject
    {
        public Dictionary<string, KeyValuePair<Type, object>> _fields;

        public ICADeletedTable1()
        { }
        public ICADeletedTable1(List<Field> fields)
        {
            _fields = new Dictionary<string, KeyValuePair<Type, object>>();
            fields.ForEach(x => _fields.Add(x.Name,
                new KeyValuePair<Type, object>(x.Type, null)));
        }

        public override bool TrySetMember(SetMemberBinder binder, object value)
        {
            if (_fields.ContainsKey(binder.Name))
            {
                var type = _fields[binder.Name].Key;
                if (value.GetType() == type)
                {
                    _fields[binder.Name] = new KeyValuePair<Type, object>(type, value);
                    return true;
                }
                else throw new Exception("Value " + value + " is not of type " + type.Name);
            }
            return false;
        }


        

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            result = _fields[binder.Name].Value;
            return true;
        }
    }



    [Table("ICAInsertedTable2")]
    public class ICAInsertedTable2 : DynamicObject
    {
        public Dictionary<string, KeyValuePair<Type, object>> _fields;

        public ICAInsertedTable2()
        { }
        public ICAInsertedTable2(List<Field> fields)
        {
            _fields = new Dictionary<string, KeyValuePair<Type, object>>();
            fields.ForEach(x => _fields.Add(x.Name,
                new KeyValuePair<Type, object>(x.Type, null)));
        }

        public override bool TrySetMember(SetMemberBinder binder, object value)
        {
            if (_fields.ContainsKey(binder.Name))
            {
                var type = _fields[binder.Name].Key;
                if (value.GetType() == type)
                {
                    _fields[binder.Name] = new KeyValuePair<Type, object>(type, value);
                    return true;
                }
                else throw new Exception("Value " + value + " is not of type " + type.Name);
            }
            return false;
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            result = _fields[binder.Name].Value;
            return true;
        }
    }


    public class Field
    {
        public string Name { get; set; }


        public Type Type { get; set; }


        public Field(string fieldName, Type fieldType)
        {
            Name = fieldName;
            Type = fieldType;
        }
    }
}
