import duckdb
from pathlib import Path

def main():
    con = duckdb.connect("./data/tpch.db")
    con.install_extension("substrait")
    con.load_extension("substrait")

    duckdb_dir = Path("./artifact/duckdb")
    duckdb_dir.mkdir(parents=True, exist_ok=True)
    
    # Read binary files.
    for i in range(1, 23):
        with open(duckdb_dir / f"{i}.bin.out", "w") as f_out:
            with open(duckdb_dir / f"{i}.bin.err", "w") as f_err:
                try:
                    with open(f"./artifact/tpch/{i}.bin", "rb") as f:
                        proto_bytes = f.read()
                        if len(proto_bytes) == 0:
                            print(f"{i}.bin error: empty file.", file=f_err)
                            continue
                        query_result = con.from_substrait(proto=proto_bytes)
                        print(query_result, file=f_out)
                except Exception as e:
                    print(f"{i}.bin error: {e}", file=f_err)
                Path(duckdb_dir / f"{i}.bin.ok").touch()



if __name__ == "__main__":
    main()
