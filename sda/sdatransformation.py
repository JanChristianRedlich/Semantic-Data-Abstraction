import json
from regraph import NXGraph, Rule, NXHierarchy
from regraph import plot_graph, plot_instance, plot_rule
from .sdatransformoperation import SDATransformOperation

class SDATransformation:
    """
    Class to handle transformations on hierarchical graph structures.
    This class provides methods to manipulate and transform graph structures
    according to specific rules and operations.
    """

    def __init__(self):
        """
        Initializes the SDATransformation class with empty lists
        for transformation operations, normalization operations, and executed rules.
        """
        self.transformOperations = []
        self.normalFormOperations = []
        self.excecutedRules = []

    def removeIrrelevantNodes(self, hirarchy):
        """
        Removes irrelevant nodes from a given hierarchy.

        :param hirarchy: The hierarchy object to transform.
        """
        rule = Rule.from_transform(hirarchy.get_graph("S"))
        rule.inject_remove_node("DELETE")
        self.do_transformation(hirarchy, "S", rule, showoutput=False)

    def removeHirarchies(self, hirarchy):
        """
        Removes specific hierarchy types (e.g., Structs and Lists) and establishes
        connections between root and fields.

        :param hirarchy: The hierarchy object to transform.
        """
        # Remove Structs and Lists
        rule = Rule.from_transform(hirarchy.get_graph("M"))
        rule.inject_remove_node("STRUCT")
        rule.inject_remove_node("ARRAY")
        self.do_transformation(hirarchy, "M", rule, showoutput=False)

        # Add Connection between Root and Fields
        pattern2 = NXGraph()
        pattern2.add_nodes_from(["ROOT", "FIELD"])
        rule = Rule.from_transform(pattern2)
        rule.inject_add_edge("ROOT", "FIELD")
        
        # Get potential Edges instances
        instances = hirarchy.find_matching("G", rule.lhs)
        self.excecutedRules.append(rule)

        # Check if types are matching
        for instance in instances:
            valide_match = True
            for key, value in instance.items():
                for node in hirarchy.get_graph("G").nodes():
                    if node == value and hirarchy.node_type("G", node)['M'] != key:
                        valide_match = False

            # Transform valid matches
            if valide_match:
                rhs_instance = hirarchy.rewrite("G", rule, instance)

    def constructTargetHirarchies(self, hirarchy, targetS):
        """
        Constructs the target hierarchies based on the provided target schema.

        :param hirarchy: The hierarchy object to transform.
        :param targetS: The target schema to guide the transformation.
        """
        # Restore Nestable Schema
        rule = Rule.from_transform(hirarchy.get_graph("M"))
        rule.inject_add_node("STRUCT")
        rule.inject_add_edge("STRUCT", "STRUCT")
        rule.inject_add_edge("ROOT", "STRUCT")
        self.do_transformation(hirarchy, "M", rule, showoutput=False)

        # Execute restoration steps
        operations = self.get_transformation_steps(targetS, [])
        operations.reverse()
        for operation in operations:
            hirarchy_name, mapping, type = operation
            mapping = self.add_hirarchy(hirarchy_name, mapping, type, hirarchy)
            if mapping != None:
                self.add_transform_operation(operation, mapping, type)
        return None

    def do_transformation(self, G, hirarchy, rule, showoutput=False):
        """
        Applies a transformation rule to a hierarchy.

        :param G: Graph object to apply the rule on.
        :param hirarchy: The hierarchy name where the rule will be applied.
        :param rule: The transformation rule to apply.
        :param showoutput: Boolean flag to display the output graph (default: False).
        """
        self.excecutedRules.append(rule)
        instances = G.find_matching(hirarchy, rule.lhs)
        print("Apply Rule to: ", len(instances), " instances")
        if showoutput:
            plot_rule(rule)
        try:
            for instance in instances:
                rhs_instance = G.rewrite(hirarchy, rule, instance, strict=False)
        except Exception as e:
            print()
        if showoutput:
            plot_graph(G.get_graph(hirarchy))

    def add_hirarchy(self, struct_name, mapping, type, hirarchy):
        """
        Adds a new hierarchy to the graph based on the provided mapping.

        :param struct_name: Name of the new structure to add.
        :param mapping: Mapping of fields to their corresponding types.
        :param type: Type of the new hierarchy (e.g., Struct or List).
        :param hirarchy: The hierarchy object to transform.
        :return: The rewritten hierarchy instance.
        """
        # Create LHS Rule
        pattern = NXGraph()
        nodes = ["root"]
        for key, value in mapping.items():
            nodes.append(key)
        pattern.add_nodes_from(nodes)
        for field, value in mapping.items():
            pattern.add_edge("root", field)

        # Transform Graph to get RHS Rule
        rule = Rule.from_transform(pattern)
        rule.inject_add_node(struct_name)
        rule.inject_add_edge("root", struct_name)
        for field, value in mapping.items():
            rule.inject_remove_edge("root", field)
            rule.inject_add_edge(struct_name, field)
        self.excecutedRules.append(rule)

        rhs_typing = {
            "S": {
                "root": "root",
            },
            "M": {}
        }
        for key, value in mapping.items():
            rhs_typing["S"][key] = value
            if key == value:
                rhs_typing["M"][key] = "STRUCT"
            else:
                rhs_typing["M"][key] = "FIELD"
        rhs_typing["M"][struct_name] = "STRUCT"

        instances = hirarchy.find_matching("G", rule.lhs)
        for instance in instances:
            valide_match = True
            for key, value in instance.items():
                for node in hirarchy.get_graph("G").nodes():
                    if node == value and key in mapping.keys() and hirarchy.node_type("G", node)['S'] != mapping[key]:
                        valide_match = False

            # Transform valid matches
            if valide_match:
                rhs_instance = hirarchy.rewrite("G", rule, instance, rhs_typing=rhs_typing)
                return rhs_instance

    def create_operation_mapping(self, sub):
        """
        Creates a mapping of operations based on the provided structure.

        :param sub: Substructure (dict or list) to create the mapping from.
        :return: A dictionary representing the mapping.
        """
        if "dict" in str(type(sub)):
            mapping = {}
            for key, value in sub.items():
                if "dict" in str(type(value)):
                    mapping[key] = key
                else:
                    mapping[key] = value
            return mapping
        elif "list" in str(type(sub)):
            mapping = {}
            for key, value in sub[0].items():
                if "dict" in str(type(value)):
                    mapping[key] = key
                else:
                    mapping[key] = value
            return mapping
        else:
            return sub

    def get_transformation_steps(self, annotation, operations=[]):
        """
        Retrieves the steps required to transform a hierarchy based on the provided annotation.

        :param annotation: The annotation defining the transformation.
        :param operations: List to accumulate transformation steps (default: empty list).
        :return: A list of transformation steps.
        """
        for key, value in annotation.items():
            if "dict" in str(type(value)):
                operations.append((key, self.create_operation_mapping(value), "dict"))
            elif "list" in str(type(value)):
                operations.append((key, self.create_operation_mapping(value), "list"))

        # Get Nested Hierarchies
        for key, value in annotation.items():
            if "dict" in str(type(value)):
                self.get_transformation_steps(value, operations)
            elif "list" in str(type(value)):
                if "dict" in str(type(value[0])):
                    self.get_transformation_steps(value[0], operations)
        return operations

    def add_flattening_operations(self, final_hirarchie):
        """
        Adds operations to flatten the hierarchy structure.

        :param final_hirarchie: The final hierarchy to flatten.
        """
        pattern3 = NXGraph()
        pattern3.add_nodes_from(["FIELD"])
        instances = final_hirarchie.find_matching("G", pattern3)
        for instance in instances:
            valide_match = True
            for key, value in instance.items():
                for node in final_hirarchie.get_graph("G").nodes():
                    if node == value and final_hirarchie.node_type("G", node)['M'] != key:
                        valide_match = False
            if valide_match:
                self.normalFormOperations.append(SDATransformOperation(
                    action="selectField",
                    field=instance['FIELD'],
                ))

    def add_transform_operation(self, operation, mapping, type):
        """
        Adds a transformation operation to the list of transformation operations.

        :param operation: The operation details (new hierarchy, connection nodes, type).
        :param mapping: Mapping of fields for the operation.
        :param type: Type of the transformation (e.g., dict or list).
        """
        new_hirarchy, connect_nodes, type = operation
        connectedFields = []

        # Rename Nodes
        for node_name, semantic_annotation in connect_nodes.items():
            self.transformOperations.append(SDATransformOperation(
                action="renameNode",
                field=mapping[node_name],
                rename=node_name
            ))
            connectedFields.append(node_name)

        if type == "dict":
            # Add Dict Hierarchy
            self.transformOperations.append(SDATransformOperation(
                action="addHirarchy",
                field=new_hirarchy,
                connect=connectedFields
            ))
        else:
            # Add List Hierarchy
            self.transformOperations.append(SDATransformOperation(
                action="nestList",
                field=new_hirarchy,
                connect=connectedFields
            ))
