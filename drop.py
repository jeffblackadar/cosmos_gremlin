
import gremlin_base_functions as gf
_gremlin_cleanup_graph = "g.V().drop()"

client = gf.connect()
gf.cleanup_graph(client)
client.close()