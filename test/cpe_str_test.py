import cpe_demo


def test_conversion_to_23_should_handle_update():
    assert cpe_demo.str_convert_cpe_22_to_23(
        "cpe:/a:microsoft:windows:2003:update1") == "cpe:2.3:a:microsoft:windows:2003:update1:*:*:*:*:*:*"


def test_conversion_to_23_should_handle_version():
    assert cpe_demo.str_convert_cpe_22_to_23("cpe:/a:microsoft:windows:2003") == "cpe:2.3:a:microsoft:windows:2003:*:*:*:*:*:*:*"


def test_conversion_to_23_should_handle_default():
    assert cpe_demo.str_convert_cpe_22_to_23("cpe:/a:microsoft:windows") == "cpe:2.3:a:microsoft:windows:*:*:*:*:*:*:*:*"


def test_conversion_to_22_should_handle_update():
    assert cpe_demo.str_convert_cpe_23_to_22(
        "cpe:2.3:a:microsoft:windows:2003:update1:*:*:*:*:*:*") == "cpe:/a:microsoft:windows:2003:update1"


def test_conversion_to_22_should_handle_version():
    assert cpe_demo.str_convert_cpe_23_to_22("cpe:2.3:a:microsoft:windows:2003:*:*:*:*:*:*:*") == "cpe:/a:microsoft:windows:2003"


def test_conversion_to_22_should_handle_default():
    assert cpe_demo.str_convert_cpe_23_to_22("cpe:2.3:a:microsoft:windows:*:*:*:*:*:*:*:*") == "cpe:/a:microsoft:windows"
