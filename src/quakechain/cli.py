
import json, sys
from . import pipeline


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    mode = "full"
    if "--incremental" in argv:
        mode = "incremental"
    if "--full" in argv:
        mode = "full"
    if "--latest" in argv:
        print(json.dumps(pipeline.latest(), ensure_ascii=False))
        return
    if "--initial" in argv:
        print(json.dumps(pipeline.initial(), ensure_ascii=False))
        return
    if "--origin" in argv or "--first" in argv:
        print(json.dumps(pipeline.origin(), ensure_ascii=False))
        return
    if "--as-of" in argv:
        index = argv.index("--as-of")
        try:
            moment = argv[index + 1]
        except IndexError:
            raise SystemExit("--as-of requires a receive timestamp")
        print(json.dumps(pipeline.as_of(moment), ensure_ascii=False))
        return
    if "--explain" in argv:
        index = argv.index("--explain")
        try:
            logical_id = argv[index + 1]
        except IndexError:
            raise SystemExit("--explain requires a logical id")
        print(json.dumps(pipeline.explain(logical_id), ensure_ascii=False))
        return
    print(json.dumps(pipeline.run(mode=mode), ensure_ascii=False))


if __name__ == "__main__":
    main()
