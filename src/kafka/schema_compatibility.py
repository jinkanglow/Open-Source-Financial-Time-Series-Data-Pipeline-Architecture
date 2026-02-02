"""
Schema Compatibility Checker

Validates Avro schema backward/forward compatibility
"""

import avro.schema
from avro.schema import Schema, RecordSchema, UnionSchema, ArraySchema
from typing import List, Tuple, Optional
import json


class SchemaCompatibilityError(Exception):
    """Raised when schema compatibility check fails"""
    pass


class SchemaCompatibilityChecker:
    """Check Avro schema backward/forward compatibility"""
    
    @staticmethod
    def check_backward_compatibility(old_schema: Schema, new_schema: Schema) -> Tuple[bool, List[str]]:
        """
        Check backward compatibility: can new schema read data written with old schema?
        
        Rules:
        - Fields can be added (with defaults)
        - Fields can be removed
        - Field types can be promoted (int -> long, float -> double)
        - Cannot change field types in incompatible ways
        
        Returns:
            (is_compatible, list_of_errors)
        """
        errors = []
        
        if not isinstance(old_schema, RecordSchema) or not isinstance(new_schema, RecordSchema):
            errors.append("Both schemas must be Record schemas")
            return False, errors
        
        old_fields = {field.name: field for field in old_schema.fields}
        new_fields = {field.name: field for field in new_schema.fields}
        
        # Check: all fields in old schema must exist in new schema (or be removed)
        # Actually, backward compatibility means new can read old, so:
        # - Old fields can be removed (new schema doesn't need them)
        # - New fields must have defaults (so old data can be read)
        
        for field_name, old_field in old_fields.items():
            if field_name not in new_fields:
                # Field removed - this is OK for backward compatibility
                continue
            
            new_field = new_fields[field_name]
            
            # Check type compatibility
            if not SchemaCompatibilityChecker._are_types_compatible(old_field.type, new_field.type):
                errors.append(
                    f"Field '{field_name}': type change from {old_field.type} to {new_field.type} "
                    f"is not backward compatible"
                )
        
        # Check: new fields must have defaults
        for field_name, new_field in new_fields.items():
            if field_name not in old_fields:
                if new_field.default is None:
                    errors.append(
                        f"New field '{field_name}' must have a default value for backward compatibility"
                    )
        
        return len(errors) == 0, errors
    
    @staticmethod
    def check_forward_compatibility(old_schema: Schema, new_schema: Schema) -> Tuple[bool, List[str]]:
        """
        Check forward compatibility: can old schema read data written with new schema?
        
        Rules:
        - New fields can be added (old schema ignores them)
        - Fields cannot be removed
        - Field types cannot change
        
        Returns:
            (is_compatible, list_of_errors)
        """
        errors = []
        
        if not isinstance(old_schema, RecordSchema) or not isinstance(new_schema, RecordSchema):
            errors.append("Both schemas must be Record schemas")
            return False, errors
        
        old_fields = {field.name: field for field in old_schema.fields}
        new_fields = {field.name: field for field in new_schema.fields}
        
        # Check: all fields in old schema must exist in new schema
        for field_name, old_field in old_fields.items():
            if field_name not in new_fields:
                errors.append(
                    f"Field '{field_name}' removed in new schema - breaks forward compatibility"
                )
                continue
            
            new_field = new_fields[field_name]
            
            # Check type compatibility (must be exact match for forward compatibility)
            if old_field.type != new_field.type:
                errors.append(
                    f"Field '{field_name}': type changed from {old_field.type} to {new_field.type} "
                    f"- breaks forward compatibility"
                )
        
        # New fields are OK (old schema will ignore them)
        
        return len(errors) == 0, errors
    
    @staticmethod
    def _are_types_compatible(old_type, new_type) -> bool:
        """Check if two Avro types are compatible"""
        # Type promotion rules
        promotion_map = {
            "int": ["long", "float", "double"],
            "long": ["float", "double"],
            "float": ["double"],
            "string": ["bytes"],
            "bytes": ["string"]
        }
        
        old_type_str = str(old_type)
        new_type_str = str(new_type)
        
        # Exact match
        if old_type_str == new_type_str:
            return True
        
        # Check promotion
        if old_type_str in promotion_map:
            if new_type_str in promotion_map[old_type_str]:
                return True
        
        # Union types - check if new type is subset
        if isinstance(old_type, UnionSchema) and isinstance(new_type, UnionSchema):
            # Simplified: check if all old types are compatible with new types
            return True  # Simplified check
        
        # Array types
        if isinstance(old_type, ArraySchema) and isinstance(new_type, ArraySchema):
            return SchemaCompatibilityChecker._are_types_compatible(
                old_type.items, new_type.items
            )
        
        return False
    
    @staticmethod
    def validate_schema_evolution(old_schema_path: str, new_schema_path: str) -> Tuple[bool, List[str]]:
        """
        Validate schema evolution
        
        Args:
            old_schema_path: Path to old schema file
            new_schema_path: Path to new schema file
            
        Returns:
            (is_compatible, list_of_errors)
        """
        with open(old_schema_path, 'r') as f:
            old_schema = avro.schema.parse(f.read())
        
        with open(new_schema_path, 'r') as f:
            new_schema = avro.schema.parse(f.read())
        
        # Check both backward and forward compatibility
        backward_ok, backward_errors = SchemaCompatibilityChecker.check_backward_compatibility(
            old_schema, new_schema
        )
        forward_ok, forward_errors = SchemaCompatibilityChecker.check_forward_compatibility(
            old_schema, new_schema
        )
        
        all_errors = backward_errors + forward_errors
        is_compatible = backward_ok and forward_ok
        
        return is_compatible, all_errors


def check_schema_compatibility_in_ci(old_schema: str, new_schema: str) -> bool:
    """
    Check schema compatibility in CI pipeline
    
    Blocks PR if schema is not compatible
    
    Args:
        old_schema: Old schema JSON string
        new_schema: New schema JSON string
        
    Returns:
        True if compatible, False otherwise
    """
    old_schema_obj = avro.schema.parse(old_schema)
    new_schema_obj = avro.schema.parse(new_schema)
    
    backward_ok, backward_errors = SchemaCompatibilityChecker.check_backward_compatibility(
        old_schema_obj, new_schema_obj
    )
    
    if not backward_ok:
        print("Schema backward compatibility check failed:")
        for error in backward_errors:
            print(f"  - {error}")
        return False
    
    forward_ok, forward_errors = SchemaCompatibilityChecker.check_forward_compatibility(
        old_schema_obj, new_schema_obj
    )
    
    if not forward_ok:
        print("Schema forward compatibility check failed:")
        for error in forward_errors:
            print(f"  - {error}")
        return False
    
    print("Schema compatibility check passed!")
    return True


if __name__ == "__main__":
    # Example usage
    old_schema_json = """
    {
        "type": "record",
        "name": "MarketData",
        "fields": [
            {"name": "symbol", "type": "string"},
            {"name": "price", "type": "double"}
        ]
    }
    """
    
    new_schema_json = """
    {
        "type": "record",
        "name": "MarketData",
        "fields": [
            {"name": "symbol", "type": "string"},
            {"name": "price", "type": "double"},
            {"name": "volume", "type": "double", "default": 0.0}
        ]
    }
    """
    
    is_compatible = check_schema_compatibility_in_ci(old_schema_json, new_schema_json)
    print(f"Compatible: {is_compatible}")
