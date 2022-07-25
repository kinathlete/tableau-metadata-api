#!/usr/bin/env python
import snowflake.connector

# Gets the version
ctx = snowflake.connector.connect(
    user='marcel.kintscher@biztory.be',
    account='pz96004.europe-west4.gcp',
    authenticator='externalbrowser',
    role='BIZTORYTEAM',
    warehouse='ANALYTICS_WH',
    database='SANDBOX_MARCELKINTSCHER',
    schema='HEH_MIGRATION'
)
cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()