import json
from regraph import NXGraph, Rule, NXHierarchy
from regraph import plot_graph, plot_instance, plot_rule

from .sdatransformoperation import SDATransformOperation

class SDATransformation:
    def __init__(self):
        self.transformOperations = []
        self.normalFormOperations = []
        self.excecutedRules = []

    def removeIrrelevantNodes(self, hirarchy):
        rule = Rule.from_transform(hirarchy.get_graph("S"))
        rule.inject_remove_node("DELETE")
        self.do_transformation(hirarchy, "S", rule, showoutput=False)
    
    def removeHirarchies(self, hirarchy):

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

        # Get potential Edges
        instances = hirarchy.find_matching("G", rule.lhs)
        self.excecutedRules.append(rule)

        # Check if Types are matching
        for instance in instances:
            valide_match = True
            for key, value in instance.items():
                for node in hirarchy.get_graph("G").nodes():
                    if node == value and hirarchy.node_type("G", node)['M'] != key:
                        valide_match = False

            # Transform valide Matches
            if valide_match:
                rhs_instance = hirarchy.rewrite("G", rule, instance)
    
    def constructTargetHirarchies(self, hirarchy, targetS):
        # Restore Nestable Shema
        rule = Rule.from_transform(hirarchy.get_graph("M"))
        rule.inject_add_node("STRUCT")
        rule.inject_add_edge("STRUCT", "STRUCT")
        rule.inject_add_edge("ROOT", "STRUCT")

        self.do_transformation(hirarchy, "M", rule, showoutput=False)

        # Excecute Restoration Steps
        operations = self.get_transformation_steps(targetS, [])
        operations.reverse()
        for operation in operations:
            hirarchy_name, mapping, type = operation
            mapping = self.add_hirarchy(hirarchy_name, mapping, type, hirarchy)
            if mapping != None:
                self.add_transform_operation(operation, mapping, type)

        return None
   
    ##############
    def do_transformation(self, G, hirarchy, rule, showoutput=False):
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
            "M": {
            }
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

            # Transform valide Matches
            if valide_match:
                rhs_instance = hirarchy.rewrite("G", rule, instance, rhs_typing=rhs_typing)
        return rhs_instance


    def create_operation_mapping(self, sub):
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

    # Get Hirchy Steps
    def get_transformation_steps(self, annotation, operations = []):
        for key, value in annotation.items():
            if "dict" in str(type(value)):
                operations.append((key, self.create_operation_mapping(value), "dict"))
            elif "list" in str(type(value)):
                operations.append((key, self.create_operation_mapping(value), "list"))
        # Get Nested Hirarchies 
        for key, value in annotation.items():
            if "dict" in str(type(value)):
                self.get_transformation_steps(value, operations)
            elif "list" in str(type(value)):
                if "dict" in str(type(value[0])):
                    self.get_transformation_steps(value[0], operations)
            
        return operations
    
    def add_flattening_operations(self, final_hirarchie):
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
                    field= instance['FIELD'],
                ))

    def add_transform_operation(self, operation, mapping, type):
        new_hirarchy, connect_nodes, type = operation
        connectedFields = []

        # Rename Nodes
        for node_name, semantic_annotation in connect_nodes.items():
            self.transformOperations.append(SDATransformOperation(
                action = "renameNode", 
                field = mapping[node_name],
                rename = node_name
            ))
            connectedFields.append(node_name)
        if type == "dict":
            # Add Dict Hirarchy
            self.transformOperations.append(SDATransformOperation(
                action = "addHirarchy", 
                field = new_hirarchy,
                connect = connectedFields
            ))
        else:
            # Add List Hirarchy
            self.transformOperations.append(SDATransformOperation(
                action = "nestList", 
                field = new_hirarchy,
                connect = connectedFields
            ))