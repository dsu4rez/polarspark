class DataType:
    def __str__(self): return self.__class__.__name__

class StringType(DataType): pass
class IntegerType(DataType): pass
class LongType(DataType): pass
class FloatType(DataType): pass
class DoubleType(DataType): pass
class BooleanType(DataType): pass
class ArrayType(DataType):
    def __init__(self, elementType, containsNull=True):
        self.elementType = elementType
        self.containsNull = containsNull
    def __str__(self): return f"ArrayType({self.elementType})"

class StructField(DataType):
    def __init__(self, name, dataType, nullable=True):
        self.name = name
        self.dataType = dataType
        self.nullable = nullable

class StructType(DataType):
    def __init__(self, fields):
        self.fields = fields
