import sys
import re
import networkx as nx
import matplotlib.pyplot as plt
import graphviz as gv

tmp_id2name = {
	0: 's1',
	1: 's2',
	2: 's3',
	3: 's4',
	4: 's5',
	5: 's6',
	6: 's7',
	7: 's8',
	8: 's9',
	9: 'c1',
	10: 'c2',
	11: 'c3'
}

class CallGroup(object):

	def __init__(self, name: str, fr, to, tot=0, req=0) -> None:
		self.name = name
		self.fr = None
		self.dest = set()
		if fr is not None:
			self.fr = fr
			self.total = tot
			self.req = req
		if to is not None:
			self.dest: set[int] = {to}
	
	def add_call(self, fr:int, to:int, tot=0, req=0):
		if fr is not None:
			self.fr = fr
			self.total = tot
			self.req = req
		if to is not None:
			self.dest.add(to)

	def __repr__(self):
		cls_str = self.name
		cls_str += str(self.fr)
		cls_str += ","
		all_des = list(self.dest)
		all_des.sort()
		cls_str += ".".join([str(des) for des in all_des])
		return cls_str
	
	def __hash__(self):
		return hash(self.__repr__())

	def __eq__(self, other: 'CallGroup'):
		return self.__repr__() == other.__repr__()


def filter_dep_traces(filename: str):
	dep_trace = []
	with open(filename, 'r') as f:
		for line in f:
			if '[IN]' in line or '[OUT]' in line:
				start = line.find('(')
				line  = line[start:]
				dep_trace.append(line)
	
	with open(filename+'.trc', 'w') as f:
		f.writelines(dep_trace)


def extract_trace_data(filename: str):
	sites = set()
	calls: dict[int, CallGroup] = {}
	
	with open(filename+'.trc', 'r') as f:
		for line in f:
			reg_exp = '|'.join(map(re.escape, ['(',')','|','/','\n']))
			splitted = re.split(reg_exp, line)
			site_id = int(splitted[1])
			direction = splitted[2]
			funcname = splitted[3]
			req = int(splitted[4])
			total = int(splitted[5])
			id = int(splitted[6], 16)

			if direction == '[IN]':
				fr = None
				to = site_id
			elif direction == '[OUT]':
				fr = site_id
				to = None
			else:
				assert(False)

			sites.add(site_id)
			if id not in calls:
				calls[id] = CallGroup(funcname, fr, to, total, req)
			else:
				calls[id].add_call(fr, to, total, req)

			
	
	return calls, sites


def deduplicate(calls: dict):
	dedup_calls = set()
	for c in calls.values():
		dedup_calls.add(c)

	return dedup_calls


def draw_dep_graph_nx(calls: 'set[CallGroup]', sites: set):
	G = nx.MultiDiGraph()
	for si in sites:
		G.add_node(si, label=tmp_id2name[si])
	
	for cg in calls:
		for des in cg.dest:
			G.add_edge(cg.fr, des, color='green' if cg.req < cg.total else 'red', label='{}/{}'.format(cg.req, cg.total))
			print(cg.fr, "->", des)

	nx.write_graphml(G, 'graph.graphml')


def draw_dep_graph_gv(calls: 'set[CallGroup]', sites: set):
	G = gv.Digraph()
	for si in sites:
		G.node(str(si), label=str(si))
	
	for cg in calls:
		for des in cg.dest:
			G.edge_attr.update(color='green' if cg.req < cg.total else 'red')
			G.edge(str(cg.fr), str(des),
				   label="{}:{}/{}".format(cg.name, cg.req, cg.total))
	
	G.view(quiet=True)


if __name__ == '__main__':
	filename = sys.argv[1]
	# filename = "copilot1c3s3r.log"
	filter_dep_traces(filename)
	calls, sites = extract_trace_data(filename)
	calls = deduplicate(calls)
	# draw_dep_graph_gv(calls, sites)
	draw_dep_graph_nx(calls, sites)
