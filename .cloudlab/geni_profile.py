"""
Runs the TPC-H benchmarks, including query and data generation.
Configurable index configuration, scale factor, etc

See https://github.com/const-sambird/tpch-psql
"""

# Import the Portal object.
import geni.portal as portal
# Import the ProtoGENI library.
import geni.rspec.pg as pg
# Import the Emulab specific extensions.
import geni.rspec.emulab as emulab

# Create a portal object,
pc = portal.Context()

'''
Some default values for the configurations
'''
DEFAULT_INDEX_CONFIGURATION = \
'''
0,p_name
'''
DEFAULT_ROUTEING_TABLE = '0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0'
DEFAULT_REPLICA_STRING = '5432,tpchdb,sam,K7wJReQcW86SUYyhXaLPvsE2jnVBGMxNHzDfbCkt5u39FTAdrq,'

pc.defineParameter('nodes', 'Number of database replicas', portal.ParameterType.INTEGER, 2)
#pc.defineParameter('trials', 'Number of benchmarks to run', portal.ParameterType.INTEGER, 10)
pc.defineParameter('config', 'Index configuration', portal.ParameterType.STRING, DEFAULT_INDEX_CONFIGURATION, longDescription='Separate each entry with a single space character.')
pc.defineParameter('routes', 'Routeing table', portal.ParameterType.STRING, DEFAULT_ROUTEING_TABLE)
pc.defineParameter('replica_str', 'Replica connection string', portal.ParameterType.STRING, DEFAULT_REPLICA_STRING)
params = pc.bindParameters()

# Create a Request object to start building the RSpec.
request = pc.makeRequestRSpec()

if params.nodes < 1:
    pc.reportError(portal.ParameterError('Invalid number of database replicas! must have at least 1'))
#if params.trials < 1:
#    pc.reportError(portal.ParameterError('Invalid number of trials! must have at least 1'))
#if params.nodes * params.trials > 190:
#    pc.reportError(portal.ParameterError('Too many nodes requested - try less trials or nodes'))
    
parsed_config = params.config.replace(' ', '\n')
    
pc.verifyParameters()

vm_count = 0

#for trial in range(params.trials):
trial = 0
# Node index-tuner
node_index_tuner = request.XenVM('benchmarker-' + str(trial + 1))
node_index_tuner.disk_image = 'urn:publicid:IDN+utah.cloudlab.us+image+qdina-PG0:tpch-psql'
node_index_tuner.hardware_type = 'c6620'
node_index_tuner.ram = 12288
iface0 = node_index_tuner.addInterface('interface-' + str(trial + 1), pg.IPv4Address('192.168.0.' + str(vm_count + 64),'255.255.255.0'))
vm_count += 1

ifaces = []
replicas = ''

for i in range(params.nodes):
    if i > 0:
        replicas += '\n'
    node = request.XenVM('replica-' + str(trial + 1) + '-' + str(i + 1))
    node.disk_image = 'urn:publicid:IDN+utah.cloudlab.us+image+qdina-PG0:pg_empty'
    iface = node.addInterface('interface-' + str(i + 1), pg.IPv4Address('192.168.0.' + str(vm_count + 64), '255.255.255.0'))
    ifaces.append(iface)
    replicas += str(i + 1) + ',192.168.0.' + str(vm_count + 64) + ',' + params.replica_str
    vm_count += 1

node_index_tuner.addService(pg.Execute(shell='bash', command='sudo -u sambird cat >/opt/tpch-psql/replicas.csv <<EOL\n' + replicas + '\nEOL'))
node_index_tuner.addService(pg.Execute(shell='bash', command='sudo -u sambird cat >/opt/tpch-psql/routes.csv <<EOL\n' + params.routes + '\nEOL'))
node_index_tuner.addService(pg.Execute(shell='bash', command='sudo -u sambird cat >/opt/tpch-psql/index_config.csv <<EOL\n' + parsed_config + '\nEOL'))
node_index_tuner.addService(pg.Execute(shell='bash', command='sudo -u sambird cp -r /proj/qdina-PG0/tpc-h /opt/tpch-psql'))

# Link link-0
link_0 = request.Link('link-' + str(trial + 1))
#link_0.Site('undefined')
link_0.addInterface(iface0)
for iface in ifaces:
    link_0.addInterface(iface)

# Print the generated rspec
pc.printRequestRSpec(request)