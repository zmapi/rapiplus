#!/usr/bin/env python3

from pprint import pprint, pformat
import re
import argparse
import keyword


################################### GLOBALS ###################################


class GlobalState:
    pass
g = GlobalState()


###############################################################################


def camelize(x):
    spl = x.split("_")
    spl = [x[0] + x[1:].lower() for x in spl]
    return "".join(spl)


def sanitize_key(x):
    if keyword.iskeyword(x):
        return sanitize_key(x + "_")
    return x


def c_int_to_py(x):
    if x.endswith("L"):
        x = x[:-1]
    if "0x" in x:
        return int(x, 16)
    return int(x, 10)


def parse_args():
    parser = argparse.ArgumentParser(
            description="python constants file code generator")
    parser.add_argument("-o", "--output",
                        type=str,
                        default="bin/constants.py",
                        help="output file name")
    parser.add_argument("-i", "--input",
                        type=str,
                        default="include/RApiPlus.h",
                        help="output file name")
    parser.add_argument("--tab-width",
                        type=int,
                        default=4,
                        help="tab width")
    parser.add_argument("--name-col-width",
                        type=int,
                        default=40,
                        help="name column width")
    args = parser.parse_args()
    return args


def add_inverse_lookup(name):

    s = f"""
\t@staticmethod
\tdef key(v):
\t\treturn {name}._inv[v]

{name}._inv = {{v: k for k, v in {name}.__dict__.items()
               if re.match(r"^[A-Z]", k)}}
"""
    g.fout.write(s[1:])


def add_c_enum(name, rex, filter=None, all_name=None, add_inv=False):
    g.fout.write(f"class {name}:\n\n")
    rex = re.compile(rex)
    if all_name:
        val_all = 0
    for line in g.lines:
        m = rex.search(line)
        if m:
            _, k, _, v = m.groups()
            if filter:
                if not filter(line, k):
                    continue
            k = sanitize_key(camelize(k))
            g.fout.write(g.name_val_fmt.format(k, "= {}".format(v)))
            if all_name:
                val_all += c_int_to_py(v)
    if all_name:
        g.fout.write(g.name_val_fmt.format(
                     all_name, "= 0x{:X}".format(val_all)))
    g.fout.write("\n")
    if add_inv:
        add_inverse_lookup(name)
    g.fout.write("\n\n")

def add_enum(name, rex, add_inv=False):
    g.fout.write(f"class {name}:\n\n")
    m = re.search(rex, g.text, re.DOTALL)
    _, data, _ = m.groups()
    data = [x.strip().replace(",", "") for x in data.splitlines()]
    # does not work if enum has value overrides!
    for v, k in enumerate(data):
        k = sanitize_key(k)
        g.fout.write(g.name_val_fmt.format(k, "= {}".format(v)))
    g.fout.write("\n")
    if add_inv:
        add_inverse_lookup(name)
    g.fout.write("\n\n")


def finalize_output(args):
    g.fout.close()
    # replace tabs with spaces
    with open(args.output) as f:
        text = f.read()
    text = text.replace("\t", " " * args.tab_width)
    with open(args.output, "w") as f:
        f.write(text)


def main():

    args = parse_args()

    g.name_val_fmt = "\t{:<" + str(args.name_col_width) + "}{}\n"

    with open(args.input) as f:
        g.text = f.read()
    g.lines = [x.strip() for x in g.text.splitlines()]
    g.lines = [x for x in g.lines if x and not x.startswith("//")]

    g.fout = open(args.output, "w")

    g.fout.write("import re\n")
    g.fout.write("\n\n")

    add_c_enum("AlertType",
               r"(const int ALERT_)([A-Z0-9_]+)(\s*=\s*)(\d+)",
               add_inv=True)
    add_c_enum("BarType",
               r"(const int BAR_TYPE_)([A-Z0-9_]+)(\s* = )([0-9x]+)")
    add_c_enum("ConnectionType",
               r"(const int )([A-Z0-9_]+?)(_CONNECTION_ID\s*=\s*)(\d+)")
    add_c_enum("DataType", r"(const int MD_)([A-Z0-9_]+)(_CB\s* = )([0-9x]+)")
    add_c_enum("ErrCode",
               r"(^#define\s+API_)([A-Z0-9_]+)(\s+)(\d+)",
               add_inv=True)
    add_c_enum("Operator",
               r"(const int OP_)([A-Z0-9_]+)(\s* = \()([0-9x]+)")
    add_enum("SearchField", r"(enum SearchField\s+{\s+)(.*?)(\s+};)")
    add_enum("SearchOperator", r"(enum SearchOperator\s+{\s+)(.*?)(\s+};)")
    add_c_enum("SubscriptionFlag",
               r"(const int MD_)([A-Z0-9_]+)(\s* = )([0-9x]+)",
               filter=lambda l, k: not k.endswith("_CB"),
               all_name="All")
    add_c_enum("UpdateType",
               r"(const int UPDATE_TYPE_)([A-Z0-9_]+)(\s* = )([0-9x]+)")
    add_c_enum("ValueState",
               r"(const int VALUE_STATE_)([A-Z0-9_]+)(\s* = )([0-9x]+)")

    finalize_output(args)


if __name__ == "__main__":
    main()
