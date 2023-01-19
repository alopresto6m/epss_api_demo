from cpe import CPE


def str_convert_cpe_22_to_23(cpe_22: str) -> str:
    # Split into at most 7 components
    components = cpe_22.split(":", 6)
    # Drop the leading '/' on product_type ('/a' -> 'a')
    components[1] = components[1][1::]
    # Rejoin the valid components by ':'
    inner_str = ":".join(components[1::])
    # Pad the remaining fields with *
    return f"cpe:2.3:{inner_str}{':*' * (12 - len(components))}"


def str_convert_cpe_23_to_22(cpe_23: str) -> str:
    # Split into at most 8 components
    components = cpe_23.split(":", 7)
    # Drop 'cpe' and '2.3', retain the middle components except '*', and drop the trailing :* string
    valid_components = filter(lambda c: c != '*', components[2:-1])
    # Rejoin the valid components by ':'
    return f"cpe:/{':'.join(valid_components)}"


def lib_convert_cpe_22_to_23(cpe_22: str) -> str:
    cpe_23 = CPE(cpe_22)
    return cpe_23.as_fs()


def lib_convert_cpe_23_to_22(cpe_23: str) -> str:
    obj = CPE(cpe_23)
    return obj.as_uri_2_3()


def main() -> None:
    # Get CPE from command line
    cpe_22 = input("Enter a CPE 2.2 string (e.g. cpe:/a:microsoft:windows:2003): ") or "cpe:/a:microsoft:windows:2003"
    if cpe_22 != "":
        cpe_23 = str_convert_cpe_22_to_23(cpe_22)
        print(f"Converted CPE 2.2 [{cpe_22}] to 2.3 [{cpe_23}]")


if __name__ == '__main__':
    main()
