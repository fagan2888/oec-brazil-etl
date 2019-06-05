import os

# Build Dimension Tables
cmd = "bamboo-cli --folder . --entry brazil_dimensions.run_dimensions --db='clickhouse-local'"
os.system(cmd)

# Build Fact Table
for f in ["EXP","IMP"]:
    for y in [str(x) for x in range(1997,2020)]:
        cmd = "bamboo-cli --folder . --entry brazil_pipeline.run_brazil --source='http-local' --flow='{}' --year='{}' --db='clickhouse-local'".format(f,y)
        os.system(cmd)