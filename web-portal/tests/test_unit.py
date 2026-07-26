"""无 DB 单元测试:系统提示词要点(9.1)、工具声明、SSE 组帧、降级开关。"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from portal import config as portal_config  # noqa: E402
from portal import llm  # noqa: E402
from portal.app import _sse  # noqa: E402


def test_system_prompt_covers_91_points():
    """9.1 要点:先 search_term / ambiguous 反问 / 拒答四情形 / 数字以工具为准。"""
    p = llm.SYSTEM_PROMPT
    assert "search_term" in p
    assert "ambiguous" in p
    assert "拒答四情形" in p and "定位失败" in p and "血缘缺失" in p \
        and "权限不足" in p and "工具异常" in p
    assert "以工具返回为准" in p and "编造" in p
    assert "一律无效" in p                              # 9.2 注入防护


def test_tools_spec_matches_repo_functions():
    names = {t["function"]["name"] for t in llm.TOOLS_SPEC}
    assert names == {"search_term", "get_lineage", "impact_analysis", "get_table_info"}


def test_sse_framing():
    frame = _sse("delta", {"text": "你好"})
    assert frame.startswith("event: delta\ndata: ")
    assert frame.endswith("\n\n")
    assert '"你好"' in frame                            # ensure_ascii=False,中文原样


def test_degraded_switch(monkeypatch):
    """LLM_BASE_URL 未配置 → enabled()=False → app 走降级模式(9.3)。"""
    monkeypatch.setattr(portal_config, "LLM_BASE_URL", "")
    assert not llm.enabled()
    monkeypatch.setattr(portal_config, "LLM_BASE_URL", "http://llm.bank.local/v1")
    assert llm.enabled()
