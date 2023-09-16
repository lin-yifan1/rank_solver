import os
import csv
from collections import defaultdict

def nested_dict():
    '''
    Return an arbitrary level dictionary
    '''
    return defaultdict(nested_dict)

class Data:
    '''
    Attributes:
        regions:
        region_resources: 
        region_temp_tags:
        users:
        user_resource_demands:
        user_temp_tags:
        inter_region_data:
        inter_group_data: 
    '''
    def __init__(self, csv_folder: str) -> None:
        self.csv_folder = csv_folder
        self.read_from_csv()


    def read_region_resources(self) -> None:
        '''
        Read regions' resource information
        '''
        file_path = os.path.join(self.csv_folder, 'cloud_provider_data.csv')
        with open(file_path, 'r') as file:
            csv_reader = csv.reader(file)

            next(csv_reader) # skip csv headers

            self.region_resources = nested_dict() # create nested dict

            for row in csv_reader:
                self.region_resources[row[0]][row[2]][row[3]] = int(row[4])

            self.regions = list(self.region_resources.keys())

    def read_region_temp_tag(self):
        '''
        Read regions' temperature tags
        '''
        file_path = os.path.join(self.csv_folder, 'geo_place.csv')
        with open(file_path, 'r') as file:
            csv_reader = csv.reader(file)

            next(csv_reader)

            self.region_temp_tags = nested_dict()

            for row in csv_reader:
                self.region_temp_tags[row[0]][row[1]] = row[2]

    def read_user_data(self):
        '''
        Read user data, including resource request, 
        access locations and temperature request
        '''
        file_path = os.path.join(self.csv_folder, 'user_data.csv')
        with open(file_path, 'r') as file:
            csv_reader = csv.reader(file)

            next(csv_reader)

            self.user_resource_demands = nested_dict()
            self.user_temp_tags = {}

            for row in csv_reader:
                az_demand = row[4].split('/')
                if az_demand != ['']:
                    az_demand = [int(demand) for demand in az_demand]
                self.user_resource_demands[row[0]][row[1]] = {'resource': int(row[2]), 'az_demand': az_demand}
                self.user_temp_tags[row[0]] = [row[5], row[6]]

            self.groups = list(self.user_resource_demands.keys())

    def read_inter_region_data(self):
        '''
        Read latency and bandwidth data between regions
        '''
        file_path = os.path.join(self.csv_folder, 'inter_region_data.csv')
        with open(file_path, 'r') as file:
            csv_reader = csv.reader(file)
        
            next(csv_reader)

            self.inter_region_data = {}

            for row in csv_reader:
                self.inter_region_data[(row[0], row[1])] = {'ltc': int(row[2]), 'bw': int(row[3])}

    def read_inter_group_data(self):
        '''
        Read latency and bandwidth data between task groups
        '''
        file_path = os.path.join(self.csv_folder, 'inter_group_data.csv')
        with open(file_path, 'r') as file:
            csv_reader = csv.reader(file)
        
            next(csv_reader)

            self.inter_group_data = {}

            for row in csv_reader:
                self.inter_group_data[(row[0], row[1])] = {'ltc': int(row[2]), 'bw': int(row[3])}


    def read_from_csv(self) -> None:
        self.read_region_resources()
        self.read_region_temp_tag()
        self.read_user_data()
        self.read_inter_region_data()
        self.read_inter_group_data()

    def total_resource(self, region: str, resource: str) -> int:
        '''
        Return the total amount of a specific kind of resource in a region
        '''
        total = 0
        for az in self.region_resources[region].keys():
            if resource not in self.region_resources[region][az]:
                continue
            total += self.region_resources[region][az][resource]
        return total
    
    def region_total_resource(self, region: str):
        '''
        Return the sum of all kinds of resource in the region
        '''
        total = 0
        for az in self.region_resources[region].keys():
            for resource in self.region_resources[region][az].keys():
                total += self.region_resources[region][az][resource]
        return total
    
    def group_total_resource(self, group: str):
        '''
        Return the sum of resource requests of a group
        '''
        total = 0
        for resource in self.user_resource_demands[group].keys():
            total += self.user_resource_demands[group][resource]['resource']
        return total



    def check_single_resource(self, region: str, group: str, resource: str) -> bool:
        '''
        Check if a single kind of resource in the region satisfies
        the group's need
        '''
        if self.user_resource_demands[group][resource]['az_demand'] == ['']:
            # no az demand
            total_resource = self.total_resource(region, resource)
            if self.user_resource_demands[group][resource]['resource'] < total_resource:
                return True
            else:
                return False
        else:
            # with az demand
            sorted_az_demand = sorted(self.user_resource_demands[group][resource]['az_demand'], reverse=True)
            az_resources = []
            for az in self.region_resources[region].keys():
                if az == 'default':
                    continue
                if resource in self.region_resources[region][az]:
                    az_resources.append(self.region_resources[region][az][resource])

            sorted_az_resources = sorted(az_resources, reverse=True)

            # assign az requests to az using a greedy approach
            if len(sorted_az_resources) < len(sorted_az_demand):
                return False
            
            for i in range(len(sorted_az_demand)):
                if sorted_az_demand[i] > sorted_az_resources[i]:
                    return False
                
            return True


    def check_node_resource(self, region: str, group: str) -> bool:
        '''
        Check if region's resources satisfy the group's resource request
        '''
        group_demand = self.user_resource_demands[group]

        for resource in group_demand.keys():
            if not self.check_single_resource(region, group, resource):
                return False
        return True
    
    def check_node_temp(self, region: str, group: str) -> bool:
        '''
        Check the temperature tag
        '''
        def temp_compare(user_temp: str, region_temp: str) -> bool:
            temp_priority = ['hot', 'warm', 'cold']
            if temp_priority.index(user_temp) < temp_priority.index(region_temp):
                return False
            else:
                return True
        user_temp_tag = self.user_temp_tags[group]
        user_loc = user_temp_tag[0]
        user_temp = user_temp_tag[1]
        region_temp = self.region_temp_tags[region][user_loc]

        return temp_compare(user_temp, region_temp)

    def check_node_constraint(self, region: str, group: str) -> bool:
        '''
        Check if region satisfies group's demands
        '''
        if self.check_node_resource(region, group) and self.check_node_temp(region, group):
            return True
        else:
            return False
        
        
    