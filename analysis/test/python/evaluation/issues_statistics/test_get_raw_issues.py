from pathlib import Path
from typing import List, Optional

import pandas as pd
import pytest
from hyperstyle.src.python.review.inspectors.inspector_type import InspectorType
from hyperstyle.src.python.review.inspectors.issue import (
    BaseIssue,
    CodeIssue,
    IssueDifficulty,
    IssueType,
    LineLenIssue,
    MaintainabilityLackIssue,
)
from analysis.src.python.evaluation.common.pandas_util import equal_df, get_solutions_df_by_file_path
from analysis.src.python.evaluation.issues_statistics.get_raw_issues import (
    _filter_issues, _get_output_path, inspect_solutions,
)
from analysis.test.python.evaluation.issues_statistics import (
    GET_RAW_ISSUES_TARGET_FILES_FOLDER, GET_RAW_ISSUES_TEST_FILES_FOLDER,
)

ORIGINAL_DF_NAME = 'original_df'
ORIGINAL_DF_CSV = f'{ORIGINAL_DF_NAME}.csv'
ORIGINAL_DF_XLSX = f'{ORIGINAL_DF_NAME}.xlsx'

ORIGINAL_DF_WITH_RAW_ISSUES_CSV = f'{ORIGINAL_DF_NAME}_with_raw_issues.csv'
ORIGINAL_DF_WITH_RAW_ISSUES_XLSX = f'{ORIGINAL_DF_NAME}_with_raw_issues.xlsx'

NEW_DF_NAME = 'new_df'

GET_OUTPUT_PATH_TEST_DATA = [
    (Path(ORIGINAL_DF_CSV), None, Path(ORIGINAL_DF_WITH_RAW_ISSUES_CSV)),
    (Path(ORIGINAL_DF_XLSX), None, Path(ORIGINAL_DF_WITH_RAW_ISSUES_XLSX)),
    (Path(ORIGINAL_DF_CSV), Path(f'{NEW_DF_NAME}.csv'), Path(f'{NEW_DF_NAME}.csv')),
    (Path(ORIGINAL_DF_CSV), Path(f'{NEW_DF_NAME}.xlsx'), Path(f'{NEW_DF_NAME}.xlsx')),
    (Path(ORIGINAL_DF_XLSX), Path(f'{NEW_DF_NAME}.csv'), Path(f'{NEW_DF_NAME}.csv')),
    (Path(ORIGINAL_DF_XLSX), Path(f'{NEW_DF_NAME}.xlsx'), Path(f'{NEW_DF_NAME}.xlsx')),
    (Path(ORIGINAL_DF_CSV), Path(NEW_DF_NAME), Path(ORIGINAL_DF_WITH_RAW_ISSUES_CSV)),
    (Path(ORIGINAL_DF_XLSX), Path(NEW_DF_NAME), Path(ORIGINAL_DF_WITH_RAW_ISSUES_XLSX)),
    (Path(ORIGINAL_DF_CSV), Path(f'{NEW_DF_NAME}/'), Path(ORIGINAL_DF_WITH_RAW_ISSUES_CSV)),
    (Path(ORIGINAL_DF_XLSX), Path(f'{NEW_DF_NAME}/'), Path(ORIGINAL_DF_WITH_RAW_ISSUES_XLSX)),
    (Path(ORIGINAL_DF_CSV), Path(f'{NEW_DF_NAME}.unknown'), Path(ORIGINAL_DF_WITH_RAW_ISSUES_CSV)),
    (Path(ORIGINAL_DF_XLSX), Path(f'{NEW_DF_NAME}.unknown'), Path(ORIGINAL_DF_WITH_RAW_ISSUES_XLSX)),
]


@pytest.mark.parametrize(('solutions_file_path', 'output_path', 'expected_output_path'), GET_OUTPUT_PATH_TEST_DATA)
def test_get_output_path(solutions_file_path: Path, output_path: Optional[Path], expected_output_path: Path):
    actual_output_path = _get_output_path(solutions_file_path, output_path)
    assert actual_output_path == expected_output_path


ISSUES_FOR_FILTERING = [
    CodeIssue(
        origin_class="MissingSwitchDefaultCheck",
        type=IssueType.ERROR_PRONE,
        description="Some description",
        file_path=Path(""),
        line_no=112,
        column_no=13,
        inspector_type=InspectorType.CHECKSTYLE,
        difficulty=IssueDifficulty.HARD,
    ),
    CodeIssue(
        origin_class="SwitchStmtsShouldHaveDefault",
        type=IssueType.ERROR_PRONE,
        description="Some description",
        file_path=Path(""),
        line_no=112,
        column_no=1,
        inspector_type=InspectorType.PMD,
        difficulty=IssueDifficulty.HARD,
    ),
    CodeIssue(
        origin_class="MagicNumberCheck",
        type=IssueType.INFO,
        description="Some description",
        file_path=Path(""),
        line_no=303,
        column_no=25,
        inspector_type=InspectorType.CHECKSTYLE,
        difficulty=IssueDifficulty.EASY,
    ),
    MaintainabilityLackIssue(
        origin_class="SomeMaintainabilityCheck",
        type=IssueType.MAINTAINABILITY,
        description="Some description",
        file_path=Path(""),
        line_no=574,
        column_no=50,
        inspector_type=InspectorType.CHECKSTYLE,
        maintainability_lack=0,
        difficulty=IssueDifficulty.HARD,
    ),
    LineLenIssue(
        origin_class="SomeLineLenCheck",
        type=IssueType.LINE_LEN,
        description="Some description",
        file_path=Path(""),
        line_no=139,
        column_no=24,
        inspector_type=InspectorType.CHECKSTYLE,
        line_len=10,
        difficulty=IssueDifficulty.EASY,
    ),
]

ISSUES_WITHOUT_DUPLICATES = [
    CodeIssue(
        origin_class="MissingSwitchDefaultCheck",
        type=IssueType.ERROR_PRONE,
        description="Some description",
        file_path=Path(""),
        line_no=112,
        column_no=13,
        inspector_type=InspectorType.CHECKSTYLE,
        difficulty=IssueDifficulty.HARD,
    ),
    CodeIssue(
        origin_class="MagicNumberCheck",
        type=IssueType.INFO,
        description="Some description",
        file_path=Path(""),
        line_no=303,
        column_no=25,
        inspector_type=InspectorType.CHECKSTYLE,
        difficulty=IssueDifficulty.EASY,
    ),
    MaintainabilityLackIssue(
        origin_class="SomeMaintainabilityCheck",
        type=IssueType.MAINTAINABILITY,
        description="Some description",
        file_path=Path(""),
        line_no=574,
        column_no=50,
        inspector_type=InspectorType.CHECKSTYLE,
        maintainability_lack=0,
        difficulty=IssueDifficulty.HARD,
    ),
    LineLenIssue(
        origin_class="SomeLineLenCheck",
        type=IssueType.LINE_LEN,
        description="Some description",
        file_path=Path(""),
        line_no=139,
        column_no=24,
        inspector_type=InspectorType.CHECKSTYLE,
        line_len=10,
        difficulty=IssueDifficulty.EASY,
    ),
]

ISSUES_WITHOUT_ZERO_MEASURE_ISSUES = [
    CodeIssue(
        origin_class="MissingSwitchDefaultCheck",
        type=IssueType.ERROR_PRONE,
        description="Some description",
        file_path=Path(""),
        line_no=112,
        column_no=13,
        inspector_type=InspectorType.CHECKSTYLE,
        difficulty=IssueDifficulty.HARD,
    ),
    CodeIssue(
        origin_class="SwitchStmtsShouldHaveDefault",
        type=IssueType.ERROR_PRONE,
        description="Some description",
        file_path=Path(""),
        line_no=112,
        column_no=1,
        inspector_type=InspectorType.PMD,
        difficulty=IssueDifficulty.HARD,
    ),
    CodeIssue(
        origin_class="MagicNumberCheck",
        type=IssueType.INFO,
        description="Some description",
        file_path=Path(""),
        line_no=303,
        column_no=25,
        inspector_type=InspectorType.CHECKSTYLE,
        difficulty=IssueDifficulty.EASY,
    ),
    LineLenIssue(
        origin_class="SomeLineLenCheck",
        type=IssueType.LINE_LEN,
        description="Some description",
        file_path=Path(""),
        line_no=139,
        column_no=24,
        inspector_type=InspectorType.CHECKSTYLE,
        line_len=10,
        difficulty=IssueDifficulty.EASY,
    ),
]

ISSUES_WITHOUT_INFO_CATEGORY = [
    CodeIssue(
        origin_class="MissingSwitchDefaultCheck",
        type=IssueType.ERROR_PRONE,
        description="Some description",
        file_path=Path(""),
        line_no=112,
        column_no=13,
        inspector_type=InspectorType.CHECKSTYLE,
        difficulty=IssueDifficulty.HARD,
    ),
    CodeIssue(
        origin_class="SwitchStmtsShouldHaveDefault",
        type=IssueType.ERROR_PRONE,
        description="Some description",
        file_path=Path(""),
        line_no=112,
        column_no=1,
        inspector_type=InspectorType.PMD,
        difficulty=IssueDifficulty.HARD,
    ),
    MaintainabilityLackIssue(
        origin_class="SomeMaintainabilityCheck",
        type=IssueType.MAINTAINABILITY,
        description="Some description",
        file_path=Path(""),
        line_no=574,
        column_no=50,
        inspector_type=InspectorType.CHECKSTYLE,
        maintainability_lack=0,
        difficulty=IssueDifficulty.HARD,
    ),
    LineLenIssue(
        origin_class="SomeLineLenCheck",
        type=IssueType.LINE_LEN,
        description="Some description",
        file_path=Path(""),
        line_no=139,
        column_no=24,
        inspector_type=InspectorType.CHECKSTYLE,
        line_len=10,
        difficulty=IssueDifficulty.EASY,
    ),
]

FILTERED_ISSUES = [
    CodeIssue(
        origin_class="MissingSwitchDefaultCheck",
        type=IssueType.ERROR_PRONE,
        description="Some description",
        file_path=Path(""),
        line_no=112,
        column_no=13,
        inspector_type=InspectorType.CHECKSTYLE,
        difficulty=IssueDifficulty.HARD,
    ),
    LineLenIssue(
        origin_class="SomeLineLenCheck",
        type=IssueType.LINE_LEN,
        description="Some description",
        file_path=Path(""),
        line_no=139,
        column_no=24,
        inspector_type=InspectorType.CHECKSTYLE,
        line_len=10,
        difficulty=IssueDifficulty.EASY,
    ),
]

FILTER_ISSUES_TEST_DATA = [
    (
        ISSUES_FOR_FILTERING,
        True,  # allow_duplicates
        True,  # allow_zero_measure_issues
        True,  # allow_info_issues
        ISSUES_FOR_FILTERING,
    ),
    (
        ISSUES_FOR_FILTERING,
        False,  # allow_duplicates
        True,  # allow_zero_measure_issues
        True,  # allow_info_issues
        ISSUES_WITHOUT_DUPLICATES,
    ),
    (
        ISSUES_FOR_FILTERING,
        True,  # allow_duplicates
        False,  # allow_zero_measure_issues
        True,  # allow_info_issues
        ISSUES_WITHOUT_ZERO_MEASURE_ISSUES,
    ),
    (
        ISSUES_FOR_FILTERING,
        True,  # allow_duplicates
        True,  # allow_zero_measure_issues
        False,  # allow_info_issues
        ISSUES_WITHOUT_INFO_CATEGORY,
    ),
    (
        ISSUES_FOR_FILTERING,
        False,  # allow_duplicates
        False,  # allow_zero_measure_issues
        False,  # allow_info_issues
        FILTERED_ISSUES,
    ),
]


@pytest.mark.parametrize(
    ('issues', 'allow_duplicates', 'allow_zero_measure_issues', 'allow_info_issues', 'expected_issues'),
    FILTER_ISSUES_TEST_DATA,
)
def test_filter_issues(
        issues: List[BaseIssue],
        allow_duplicates: bool,
        allow_zero_measure_issues: bool,
        allow_info_issues: bool,
        expected_issues: List[BaseIssue],
):
    assert _filter_issues(issues, allow_duplicates, allow_zero_measure_issues, allow_info_issues) == expected_issues


TEST_CORRECT_OUTPUT_DATA = [
    ('test_fragment_per_language.csv', 'target_fragment_per_language.csv'),
    ('test_incorrect_language.csv', 'target_incorrect_language.csv'),
    ('test_incorrect_code.csv', 'target_incorrect_code.csv'),
    ('test_rows_with_null.csv', 'target_rows_with_null.csv'),
]


@pytest.mark.parametrize(('test_file', 'target_file'), TEST_CORRECT_OUTPUT_DATA)
def test_correct_output(test_file: str, target_file: str):
    solutions_file_path = Path(GET_RAW_ISSUES_TEST_FILES_FOLDER / test_file)
    solutions = get_solutions_df_by_file_path(solutions_file_path)

    test_dataframe = inspect_solutions(
        solutions,
        solutions_file_path,
        allow_duplicates=False,
        allow_info_issues=False,
        allow_zero_measure_issues=False,
        to_save_path=False,
    )

    target_dataframe = pd.read_csv(GET_RAW_ISSUES_TARGET_FILES_FOLDER / target_file)

    assert equal_df(target_dataframe, test_dataframe)
