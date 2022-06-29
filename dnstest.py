import dns
import dns.resolver

result = dns.resolver.query('cert-neo4j-core.hpcc-nonprod.scival.com', 'A')
for ipval in result:
    print('IP', ipval.to_text())
