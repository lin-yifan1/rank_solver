import csv
import time
import random
import networkx as nx
from data.read import Data
from itertools import islice
from copy import copy, deepcopy
from rank_solver.rank import random_walk_rank

class Solver(Data):
    def __init__(self, csv_folder):
        super().__init__(csv_folder)
        self.create_region_graph()
        self.create_group_graph()

    def create_region_graph(self):
        self.region_graph = nx.Graph()
        for region in self.regions:
            attr_dict = {'resource': self.region_resources[region], 'temp': self.region_temp_tags[region]}
            self.region_graph.add_nodes_from([(region, attr_dict)])
        for edge, attributes in self.inter_region_data.items():
            self.region_graph.add_edge(edge[0], edge[1], **attributes)

    def create_group_graph(self):
        self.group_graph = nx.Graph()
        for group in self.groups:
            attr_dict = {'resource': self.user_resource_demands[group], 'temp': self.user_temp_tags[group]}
            self.group_graph.add_nodes_from([(group, attr_dict)])
        for edge, attributes in self.inter_group_data.items():
            self.group_graph.add_edge(edge[0], edge[1], **attributes)

    def region_incident_bw(self, region: str):
        '''
        Return the sum of the bandwidth of the edges incident to the region
        '''
        total = 0
        edges = self.region_graph.edges(region)
        for edge in edges:
            total += self.region_graph.edges[edge]['bw']
        return total
    
    def group_incident_bw(self, group: str):
        '''
        Return the sum of the bandwidth of the edges incident to the group
        '''
        total = 0
        edges = self.group_graph.edges(group)
        for edge in edges:
            total += self.group_graph.edges[edge]['bw']
        return total
    
    def node_placement(self, ranked_regions: list, ranked_groups: list, placement_result: dict) -> bool:
        placement_result['node_placement'] = {}
        ranked_regions = copy(ranked_regions)
        for group in ranked_groups:
            for region in ranked_regions:
                if self.check_node_constraint(region, group):
                    placement_result['node_placement'][group] = region
                    ranked_regions.remove(region) # two different groups cannot be placed in the same region
                    break
            # check if group has been placed properly
            if group not in placement_result['node_placement']:
                return False
        return True
    
    def check_path_constraint(self, region_graph: nx.Graph, path: list, bw_demand, ltc_demand) -> bool:
        path_pairs = [(path[i], path[i+1]) for i in range(len(path)-1)]
        ltc = 0
        for pair in path_pairs:
            bw = region_graph.edges[pair]['bw']
            if bw < bw_demand:
                return False
            ltc += region_graph.edges[pair]['ltc']
        if ltc < ltc_demand:
            return True
        else:
            return False
        
    def update_path_bw(self, region_graph, path: list, bw_demand) -> None:
        path_pairs = [(path[i], path[i+1]) for i in range(len(path)-1)]
        for pair in path_pairs:
            region_graph.edges[pair]['bw'] -= bw_demand

    def link_placement(self, placement_result: dict) -> bool:
        region_graph = deepcopy(self.region_graph)
        placement_result['link_placement'] = {}
        for group_pair in self.inter_group_data.keys():
            # inter_group_data contains links pointing to group themselves
            if group_pair[0] == group_pair[1]:
                continue
            region_1 = placement_result['node_placement'][group_pair[0]]
            region_2 = placement_result['node_placement'][group_pair[1]]
            ltc_demand = self.group_graph.edges[group_pair]['ltc']
            bw_demand = self.group_graph.edges[group_pair]['bw']
            # inspect 5 shortest paths
            shortest_paths = list(islice(nx.shortest_simple_paths(self.region_graph, region_1, region_2, weight='ltc'), 10))
            for path in shortest_paths:
                if self.check_path_constraint(region_graph, path, bw_demand, ltc_demand):
                    placement_result['link_placement'][group_pair] = path
                    self.update_path_bw(region_graph, path, bw_demand)
                    break
            # check if the link has been placed properly
            if group_pair not in placement_result['link_placement']:
                return False
        return True
                
    def place(self, ranked_regions: list, ranked_groups: list, placement_result: dict) -> bool:
        if not self.node_placement(ranked_regions, ranked_groups, placement_result):
            return False
        if not self.link_placement(placement_result):
            return False
        return True

    def solve(self) -> None:
        ranked_regions, ranked_groups = random_walk_rank(self)
        placement_result = {}

        with open('summary.csv', mode='w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)

            csv_writer.writerow(["running_time", "solution"])
            start_time = time.time()
            # iteratively remove the first allocated region to get new solutions
            while self.place(ranked_regions, ranked_groups, placement_result) == True:
                end_time =time.time()
                csv_writer.writerow([end_time-start_time, placement_result])
                ranked_regions.remove(placement_result['node_placement'][ranked_groups[0]])
                self.region_graph.remove_node(placement_result['node_placement'][ranked_groups[0]])
                placement_result = {}
                start_time = time.time()
            
    