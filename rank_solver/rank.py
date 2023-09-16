import numpy as np
import networkx as nx
from typing import Tuple

def rank(graph: nx.Graph, node_total_resource, node_incident_bw, epsilon=0.0001, p_J_u=0.15, p_F_u=0.85):
    node_total_resource = np.array(node_total_resource)
    node_incident_bw = np.array(node_incident_bw)

    h_u = node_total_resource * node_incident_bw

    nr = h_u / h_u.sum()

    P_J_u_v = np.tile(nr, (graph.number_of_nodes(), 1))

    P_F_u_v = np.tile(h_u, (graph.number_of_nodes(), 1))

    node_list = list(graph.nodes)
    for i in range(graph.number_of_nodes()):
        neighbors = list(graph.neighbors(node_list[i]))
        neighbor_index = [node_list.index(neighbor) for neighbor in neighbors]
        h_w_sum = sum(h_u[index] for index in neighbor_index)
        P_F_u_v[i] = P_F_u_v[i] / h_w_sum

    T_matrix = (P_J_u_v * p_J_u + P_F_u_v * p_F_u).T
    delta = np.inf
    nr = np.expand_dims(nr, axis=0).T
    while(delta >= epsilon):
        new_nr = T_matrix @ nr
        delta = np.linalg.norm(new_nr - nr)
        nr = new_nr
    nr = list(np.squeeze(nr.T, axis=0))

    combined = sorted(zip(node_list, nr), key=lambda x: x[1], reverse=True)
    node_list, nr = zip(*combined)

    return list(node_list)



def random_walk_rank(solver, epsilon=0.0001, p_J_u=0.15, p_F_u=0.85):
    '''
    Random walk ranking which returns the ranked region list and group list
    '''
    region_total_resource = [solver.region_total_resource(region) for region in solver.regions]
    group_total_resource = [solver.group_total_resource(group) for group in solver.groups]
    region_incident_bw = [solver.region_incident_bw(region) for region in solver.regions]
    group_incident_bw = [solver.group_incident_bw(group) for group in solver.groups]

    ranked_regions = rank(solver.region_graph, region_total_resource, region_incident_bw, epsilon, p_J_u, p_F_u)
    ranked_groups = rank(solver.group_graph, group_total_resource, group_incident_bw, epsilon, p_J_u, p_F_u)

    return ranked_regions, ranked_groups


