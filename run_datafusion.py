from datafusion.context import SessionConfig, SessionContext
from datafusion import substrait as ss
from pathlib import Path

def main():
    # datafusion.sql_parser.enable_ident_normalization doesn't seem to work.
    config = (
        SessionConfig()
        .set("datafusion.execution.parquet.schema_force_view_types", "false")
    )
    ctx = SessionContext(config)
    ctx.register_parquet('"CUSTOMER"', "./data/customer_upper.parquet")
    ctx.register_parquet('"LINEITEM"', "./data/lineitem_upper.parquet")
    ctx.register_parquet('"NATION"', "./data/nation_upper.parquet")
    ctx.register_parquet('"ORDERS"', "./data/orders_upper.parquet")
    ctx.register_parquet('"PART"', "./data/part_upper.parquet")
    ctx.register_parquet('"PARTSUPP"', "./data/partsupp_upper.parquet")
    ctx.register_parquet('"REGION"', "./data/region_upper.parquet")
    ctx.register_parquet('"SUPPLIER"', "./data/supplier_upper.parquet")

    datafusion_dir = Path("./artifact/datafusion")
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
                    print(f"{i}.bin error: {e}", file=f_err)
                Path(datafusion_dir / f"{i}.bin.ok").touch()



if __name__ == "__main__":
    main()
