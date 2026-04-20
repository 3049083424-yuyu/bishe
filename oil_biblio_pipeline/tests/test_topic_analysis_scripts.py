from pathlib import Path

import pytest

import build_topic_evolution_lda as topic_lda
import build_topic_institution_profile_analysis as inst_profile


def test_build_output_paths_supports_dataset_tag():
    paths = topic_lda.build_output_paths(Path("D:/tmp/topic_output"), "dual_key")

    assert paths["topic_strength"].name == "topic_strength_dual_key_2011_2025.csv"
    assert paths["topic_assignment"].name == "topic_document_assignment_dual_key_2011_2025.csv"
    assert paths["summary"].name == "topic_evolution_summary_dual_key_2011_2025.txt"


@pytest.mark.parametrize(
    ("raw_token", "expected"),
    [
        ("reservoir", "\u50a8\u5c42"),
        ("\u50a8\u5c42", "\u50a8\u5c42"),
        ("hydraulic_fracturing", "\u6c34\u529b\u538b\u88c2"),
        ("\u538b\u88c2", "\u6c34\u529b\u538b\u88c2"),
        ("shale_gas", "\u9875\u5ca9\u6c14"),
        ("\u9875\u5ca9\u6c14", "\u9875\u5ca9\u6c14"),
        ("porosity", "\u5b54\u9699\u5ea6"),
        ("wellbore", "\u4e95\u7b52"),
        ("asphaltene", "\u6ca5\u9752\u8d28"),
        ("carbonate", "\u78b3\u9178\u76d0\u5ca9"),
    ],
)
def test_canonicalize_topic_term_unifies_chinese_and_english(raw_token, expected):
    assert topic_lda.canonicalize_topic_term(raw_token) == expected


def test_topic_label_prefers_chinese_terms():
    label = topic_lda.topic_label_from_terms(
        [
            "\u6c34\u529b\u538b\u88c2",
            "\u50a8\u5c42",
            "porosity",
            "\u9875\u5ca9\u6c14",
            "wellbore",
        ]
    )

    assert label == "\u6c34\u529b\u538b\u88c2 / \u50a8\u5c42 / \u9875\u5ca9\u6c14"


def test_extract_norm_names_from_row_prefers_clean_institution_norm():
    row = {
        "source_db": "MERGED",
        "institution": "Department of Earth Sciences, University of X",
        "institution_extracted": "Department of Earth Sciences, University of X",
        "institution_norm": "\u4e2d\u56fd\u77f3\u6cb9\u5927\u5b66\uff08\u5317\u4eac\uff09 | \u4e2d\u56fd\u79d1\u5b66\u9662",
    }

    names = inst_profile.extract_norm_names_from_row(row, {})

    assert names == ["\u4e2d\u56fd\u77f3\u6cb9\u5927\u5b66(\u5317\u4eac)", "\u4e2d\u56fd\u79d1\u5b66\u9662"]


def test_extract_norm_names_from_row_maps_parent_org_and_filters_fragments():
    row = {
        "source_db": "MERGED",
        "institution": "",
        "institution_extracted": "",
        "institution_norm": (
            "basin and reservoir research center, china university of petroleum"
            " | beijing research and development center, sinopec lubricant company"
            " | 1037LuoyuRd"
            ' | "沉积储层重点实验室'
            ' | "低品位能源利用技术及系'
        ),
    }
    raw_to_norm = {
        "Basin and Reservoir Research Center, China University of Petroleum": "\u4e2d\u56fd\u77f3\u6cb9\u5927\u5b66\uff08\u5317\u4eac\uff09",
        "Sinopec Lubricant Company": "Sinopec Lubricant Co",
    }

    names = inst_profile.extract_norm_names_from_row(row, raw_to_norm)

    assert names == ["Sinopec Lubricant Co", "\u4e2d\u56fd\u77f3\u6cb9\u5927\u5b66(\u5317\u4eac)", "\u6c89\u79ef\u50a8\u5c42\u91cd\u70b9\u5b9e\u9a8c\u5ba4"]


def test_extract_norm_names_from_row_rejects_mixed_translation_artifacts():
    row = {
        "source_db": "MERGED",
        "institution": "",
        "institution_extracted": "",
        "institution_norm": "beijing research and development center, sinopec lubricant company | key laboratory for advanced technology in environmental protection of jiangsu province, yancheng institute of technology",
    }
    raw_to_norm = {
        "Beijing Research and Development Center": "\u5317\u4eac\u7814\u7a76and\u5f00\u53d1\u4e2d\u5fc3",
        "Sinopec Lubricant Company": "Sinopec Lubricant Co",
        "Beijing Research and Development Center, Sinopec Lubricant Company": "Beijing Research and Development Center, Sinopec Lubricant Company",
        "Yancheng Institute of Technology": "yancheng\u7814\u7a76\u6240\u6280\u672f\u7814\u7a76\u6240",
        "Key Laboratory for Advanced Technology in Environmental Protection of Jiangsu Province, Yancheng Institute of Technology": "Yancheng\u7814\u7a76\u6240of\u6280\u672f",
    }

    names = inst_profile.extract_norm_names_from_row(row, raw_to_norm)

    assert names == ["Sinopec Lubricant Co", "yancheng institute of technology"]


@pytest.mark.parametrize(
    ("institution_name", "expected_type"),
    [
        ("China Sinopec Co.", "\u4f01\u4e1a\u7814\u53d1\u4e2d\u5fc3"),
        ("SINOPEC East China Co", "\u4f01\u4e1a\u7814\u53d1\u4e2d\u5fc3"),
        ("Organization of the Petroleum Exporting Countries", "\u56fd\u9645\u7ec4\u7ec7"),
        ("International Energy Agency", "\u56fd\u9645\u7ec4\u7ec7"),
    ],
)
def test_classify_institution_type_handles_international_boundaries(institution_name, expected_type):
    assert inst_profile.classify_institution_type(institution_name)[0] == expected_type


def test_classify_institution_type_does_not_misread_inc_inside_other_words():
    institution_name = "Key Laboratory for Advanced Technology in Environmental Protection of Jiangsu Province, Yancheng Institute of Technology"

    assert inst_profile.classify_institution_type(institution_name)[0] == "\u9ad8\u6821/\u79d1\u7814\u9662\u6240"


@pytest.mark.parametrize(
    ("raw_token", "expected"),
    [
        ("ratio", ""),
        ("studied", ""),
        ("production", ""),
        ("sag", "\u51f9\u9677"),
        ("co2", "\u4e8c\u6c27\u5316\u78b3"),
        ("\u9875\u5ca9\u6c14", "\u9875\u5ca9\u6c14"),
    ],
)
def test_clean_presentation_topic_term_filters_generic_terms_and_keeps_domain_terms(raw_token, expected):
    assert topic_lda.clean_presentation_topic_term(raw_token) == expected
