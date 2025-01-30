from datafusion.context import SessionConfig, SessionContext
from datafusion import substrait as ss
from pathlib import Path

def main():
    config = (
        SessionConfig()
        .set("datafusion.sql_parser.enable_ident_normalization", "false")
        .set("datafusion.execution.parquet.schema_force_view_types", "false")
    )
    ctx = SessionContext(config)
    ctx.register_parquet('customer', "./data/customer.parquet")
    ctx.register_parquet('lineitem', "./data/lineitem.parquet")
    ctx.register_parquet('nation', "./data/nation.parquet")
    ctx.register_parquet('orders', "./data/orders.parquet")
    ctx.register_parquet('part', "./data/part.parquet")
    ctx.register_parquet('partsupp', "./data/partsupp.parquet")
    ctx.register_parquet('region', "./data/region.parquet")
    ctx.register_parquet('supplier', "./data/supplier.parquet")

    datafusion_dir = Path("./artifact/datafusion_ident")
    datafusion_dir.mkdir(parents=True, exist_ok=True)
    
    # Read binary files.
    for i in range(1, 23):
        with open(datafusion_dir / f"{i}.bin.out", "w") as f_out:
            with open(datafusion_dir / f"{i}.bin.err", "w") as f_err:
                try:
                    with open(f"./artifact/tpch/{i}.bin", "rb") as f:
                        proto_bytes = f.read()
                        if len(proto_bytes) == 0:
                            print(f"{i}.bin error: empty file.", file=f_err)
                            continue

                        substrait_plan = ss.Serde.deserialize_bytes(proto_bytes)
                        df_logical_plan = ss.Consumer.from_substrait_plan(ctx, substrait_plan)
                        df = ctx.create_dataframe_from_logical_plan(df_logical_plan).to_pandas()
                        df.to_csv(f_out, index=False)
                except BaseException as e:
                    raise e
                    print(f"{i}.bin error: {e}", file=f_err)
                Path(datafusion_dir / f"{i}.bin.ok").touch()



if __name__ == "__main__":
    main()
