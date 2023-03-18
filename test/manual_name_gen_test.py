import name_gen


def test_gen_names_manual_should_include_all_suffixes():
    assert name_gen.gen_names_manual("Coca Cola") == \
           {"Coca Cola", "Coca Cola, Inc.", "Coca Cola, LLC", "Coca Cola, LLP",
            "Coca Cola Incorporated", "Coca Cola AG", "Coca Cola GmbH"}
