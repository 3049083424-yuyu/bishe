from topic_evolution_pipeline import (
    INPUT_ENCODING,
    OUTPUT_ENCODING,
    PERIODS,
    build_document_signature,
    build_output_paths,
    canonicalize_topic_term,
    clean_presentation_topic_term,
    compact_text,
    main,
    period_label_from_year,
    topic_label_from_terms,
)


__all__ = [
    "INPUT_ENCODING",
    "OUTPUT_ENCODING",
    "PERIODS",
    "build_document_signature",
    "build_output_paths",
    "canonicalize_topic_term",
    "clean_presentation_topic_term",
    "compact_text",
    "main",
    "period_label_from_year",
    "topic_label_from_terms",
]


if __name__ == "__main__":
    main()
