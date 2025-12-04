"""
AI-powered data detector using OpenAI for intelligent data structure analysis.
Analyzes HTML and JSON to propose extraction strategies.
"""

import os
import json
import hashlib
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field

from ..utils.logger import get_logger


@dataclass
class ExtractionStrategy:
    """Proposed extraction strategy from LLM analysis."""
    strategy_type: str  # "api_json", "dom_table", "js_object", "hybrid"
    confidence: float
    data_source: Dict[str, Any]
    field_mappings: Dict[str, str]
    description: str
    warnings: List[str] = field(default_factory=list)


@dataclass
class DetectionResult:
    """Result of data detection analysis."""
    detected_sources: List[Dict[str, Any]]
    recommended_strategy: Optional[ExtractionStrategy]
    all_strategies: List[ExtractionStrategy]
    raw_analysis: Optional[str] = None


class DataDetector:
    """
    AI-powered detector for finding and mapping data structures.
    Uses OpenAI API to analyze HTML/JSON and propose extraction strategies.
    """
    
    # Cache for LLM responses
    _response_cache: Dict[str, str] = {}
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "gpt-3.5-turbo",
        use_cache: bool = True,
    ):
        """
        Initialize the data detector.
        
        Args:
            api_key: OpenAI API key (uses OPENAI_API_KEY env var if not provided)
            model: Model to use (gpt-3.5-turbo or gpt-4)
            use_cache: Whether to cache LLM responses
        """
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.model = model
        self.use_cache = use_cache
        self.logger = get_logger()
        self._client = None
    
    @property
    def client(self):
        """Lazy initialization of OpenAI client."""
        if self._client is None:
            if not self.api_key:
                raise ValueError(
                    "OpenAI API key not found. Set OPENAI_API_KEY environment variable "
                    "or pass api_key to DataDetector."
                )
            try:
                from openai import OpenAI
                self._client = OpenAI(api_key=self.api_key)
            except ImportError:
                raise ImportError("OpenAI package required. Install with: pip install openai")
        return self._client
    
    def _get_cache_key(self, content: str, prompt_type: str) -> str:
        """Generate a cache key for content."""
        content_hash = hashlib.md5(content[:1000].encode()).hexdigest()
        return f"{prompt_type}_{content_hash}"
    
    def _call_llm(self, prompt: str, system_prompt: str) -> str:
        """Call the LLM with caching."""
        cache_key = self._get_cache_key(prompt, system_prompt[:50])
        
        if self.use_cache and cache_key in self._response_cache:
            self.logger.debug("Using cached LLM response")
            return self._response_cache[cache_key]
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.1,
                max_tokens=2000,
            )
            
            result = response.choices[0].message.content
            
            if self.use_cache:
                self._response_cache[cache_key] = result
            
            return result
        
        except Exception as e:
            self.logger.error(f"LLM call failed: {e}")
            raise
    
    def analyze_json(
        self,
        json_data: Any,
        context: str = "",
    ) -> DetectionResult:
        """
        Analyze JSON data structure and propose extraction strategy.
        
        Args:
            json_data: JSON data to analyze
            context: Additional context about the data source
        
        Returns:
            DetectionResult with proposed strategies
        """
        # Convert to string for analysis (truncate if too long)
        if isinstance(json_data, (dict, list)):
            json_str = json.dumps(json_data, indent=2)
        else:
            json_str = str(json_data)
        
        # Truncate for LLM
        max_length = 3000
        if len(json_str) > max_length:
            json_str = json_str[:max_length] + "\n... (truncated)"
        
        system_prompt = """You are a data extraction expert. Analyze JSON data structures 
and identify financial time-series data patterns. Output your analysis as JSON."""
        
        prompt = f"""Analyze this JSON data and identify:
1. The type of data structure (array of objects, nested object, etc.)
2. Fields that look like dates/timestamps
3. Fields that look like numeric metrics (prices, volumes, etc.)
4. The best path to the main data array if nested

Context: {context}

JSON Data:
{json_str}

Respond with a JSON object like:
{{
    "structure_type": "array_of_objects" | "nested_data" | "parallel_arrays",
    "data_path": "path.to.data.array" or null,
    "date_fields": ["field1", "field2"],
    "numeric_fields": ["field1", "field2"],
    "field_mappings": {{
        "date": "original_field_name",
        "metric1": "original_field_name"
    }},
    "confidence": 0.0-1.0,
    "notes": "any important observations"
}}"""
        
        try:
            response = self._call_llm(prompt, system_prompt)
            
            # Parse the response
            # Find JSON in the response
            json_match = self._extract_json(response)
            if json_match:
                analysis = json.loads(json_match)
            else:
                analysis = {"error": "Could not parse LLM response", "raw": response}
            
            # Build strategy from analysis
            strategy = self._build_strategy_from_analysis(analysis, "api_json")
            
            return DetectionResult(
                detected_sources=[{
                    "type": "json",
                    "structure": analysis.get("structure_type"),
                    "data_path": analysis.get("data_path"),
                }],
                recommended_strategy=strategy,
                all_strategies=[strategy] if strategy else [],
                raw_analysis=response,
            )
        
        except Exception as e:
            self.logger.error(f"JSON analysis failed: {e}")
            return self._fallback_json_analysis(json_data)
    
    def analyze_html(
        self,
        html: str,
        context: str = "",
    ) -> DetectionResult:
        """
        Analyze HTML structure to find data tables or embedded data.
        
        Args:
            html: HTML content to analyze
            context: Additional context
        
        Returns:
            DetectionResult with proposed strategies
        """
        # Extract relevant parts of HTML for analysis
        html_snippet = self._extract_html_snippet(html)
        
        system_prompt = """You are a web scraping expert. Analyze HTML to identify 
data tables and embedded data structures. Output your analysis as JSON."""
        
        prompt = f"""Analyze this HTML and identify:
1. Data tables with financial/numeric data
2. JavaScript variables containing data (look for script tags)
3. API endpoints referenced in the code
4. CSS selectors for data elements

Context: {context}

HTML Snippet:
{html_snippet}

Respond with a JSON object like:
{{
    "tables": [
        {{"selector": "#table-id", "description": "...", "has_headers": true}}
    ],
    "js_data": [
        {{"variable": "window.chartData", "description": "..."}}
    ],
    "api_hints": [
        {{"url_pattern": "/api/...", "description": "..."}}
    ],
    "recommended_approach": "dom_table" | "js_object" | "api_json",
    "confidence": 0.0-1.0,
    "notes": "any important observations"
}}"""
        
        try:
            response = self._call_llm(prompt, system_prompt)
            
            json_match = self._extract_json(response)
            if json_match:
                analysis = json.loads(json_match)
            else:
                analysis = {"error": "Could not parse LLM response", "raw": response}
            
            strategies = []
            
            # Build strategies based on findings
            if analysis.get("tables"):
                for table in analysis["tables"]:
                    strategies.append(ExtractionStrategy(
                        strategy_type="dom_table",
                        confidence=analysis.get("confidence", 0.5),
                        data_source={"type": "table", "selector": table.get("selector")},
                        field_mappings={},
                        description=table.get("description", "HTML table"),
                    ))
            
            if analysis.get("js_data"):
                for js in analysis["js_data"]:
                    strategies.append(ExtractionStrategy(
                        strategy_type="js_object",
                        confidence=analysis.get("confidence", 0.5),
                        data_source={"type": "js_object", "variable": js.get("variable")},
                        field_mappings={},
                        description=js.get("description", "JavaScript data"),
                    ))
            
            recommended = strategies[0] if strategies else None
            
            return DetectionResult(
                detected_sources=[
                    {"type": "table", **t} for t in analysis.get("tables", [])
                ] + [
                    {"type": "js_object", **j} for j in analysis.get("js_data", [])
                ],
                recommended_strategy=recommended,
                all_strategies=strategies,
                raw_analysis=response,
            )
        
        except Exception as e:
            self.logger.error(f"HTML analysis failed: {e}")
            return self._fallback_html_analysis(html)
    
    def propose_field_mappings(
        self,
        field_names: List[str],
        target_schema: Dict[str, str],
        context: str = "",
    ) -> Dict[str, str]:
        """
        Propose field mappings from source fields to target schema.
        
        Args:
            field_names: Source field names
            target_schema: Target schema with descriptions
            context: Additional context
        
        Returns:
            Dict mapping target fields to source fields
        """
        system_prompt = """You are a data mapping expert. Map source field names 
to target schema fields based on semantic meaning."""
        
        prompt = f"""Map these source fields to the target schema:

Source fields: {field_names}

Target schema:
{json.dumps(target_schema, indent=2)}

Context: {context}

Respond with a JSON object mapping target field names to source field names:
{{
    "target_field": "source_field",
    ...
}}

Only include mappings where you're confident. Use null for unmappable fields."""
        
        try:
            response = self._call_llm(prompt, system_prompt)
            
            json_match = self._extract_json(response)
            if json_match:
                return json.loads(json_match)
            return {}
        
        except Exception as e:
            self.logger.error(f"Field mapping failed: {e}")
            return self._fallback_field_mapping(field_names, target_schema)
    
    def _extract_json(self, text: str) -> Optional[str]:
        """Extract JSON from LLM response text."""
        import re
        
        # Try to find JSON block
        patterns = [
            r"```json\s*([\s\S]*?)\s*```",
            r"```\s*([\s\S]*?)\s*```",
            r"(\{[\s\S]*\})",
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    json.loads(match.group(1))
                    return match.group(1)
                except json.JSONDecodeError:
                    continue
        
        return None
    
    def _extract_html_snippet(self, html: str, max_length: int = 3000) -> str:
        """Extract relevant parts of HTML for analysis."""
        from bs4 import BeautifulSoup
        
        soup = BeautifulSoup(html, "lxml")
        
        # Remove scripts and styles (except data scripts)
        for element in soup.find_all(["style", "link"]):
            element.decompose()
        
        # Keep script tags that might contain data
        for script in soup.find_all("script"):
            content = script.string or ""
            if not any(kw in content.lower() for kw in ["data", "chart", "series", "config"]):
                script.decompose()
        
        # Focus on main content areas
        main_content = soup.find(["main", "article", "div[id*='content']", "div[class*='content']"])
        if main_content:
            text = str(main_content)
        else:
            text = str(soup)
        
        if len(text) > max_length:
            text = text[:max_length] + "\n... (truncated)"
        
        return text
    
    def _build_strategy_from_analysis(
        self,
        analysis: dict,
        strategy_type: str,
    ) -> Optional[ExtractionStrategy]:
        """Build an extraction strategy from LLM analysis."""
        if "error" in analysis:
            return None
        
        return ExtractionStrategy(
            strategy_type=strategy_type,
            confidence=analysis.get("confidence", 0.5),
            data_source={
                "type": strategy_type,
                "data_path": analysis.get("data_path"),
            },
            field_mappings=analysis.get("field_mappings", {}),
            description=analysis.get("notes", "Auto-detected strategy"),
        )
    
    def _fallback_json_analysis(self, json_data: Any) -> DetectionResult:
        """Fallback analysis when LLM is unavailable."""
        self.logger.info("Using fallback JSON analysis (no LLM)")
        
        # Basic heuristic analysis
        detected_sources = []
        
        if isinstance(json_data, list) and json_data:
            if isinstance(json_data[0], dict):
                fields = list(json_data[0].keys())
                detected_sources.append({
                    "type": "array_of_objects",
                    "fields": fields,
                    "row_count": len(json_data),
                })
        
        elif isinstance(json_data, dict):
            for key, value in json_data.items():
                if isinstance(value, list) and len(value) > 5:
                    detected_sources.append({
                        "type": "nested_array",
                        "path": key,
                        "row_count": len(value),
                    })
        
        return DetectionResult(
            detected_sources=detected_sources,
            recommended_strategy=None,
            all_strategies=[],
        )
    
    def _fallback_html_analysis(self, html: str) -> DetectionResult:
        """Fallback analysis when LLM is unavailable."""
        self.logger.info("Using fallback HTML analysis (no LLM)")
        
        from bs4 import BeautifulSoup
        
        soup = BeautifulSoup(html, "lxml")
        detected_sources = []
        
        # Find tables
        for i, table in enumerate(soup.find_all("table")):
            rows = len(table.find_all("tr"))
            detected_sources.append({
                "type": "table",
                "selector": f"table:nth-of-type({i+1})",
                "row_count": rows,
            })
        
        return DetectionResult(
            detected_sources=detected_sources,
            recommended_strategy=None,
            all_strategies=[],
        )
    
    def _fallback_field_mapping(
        self,
        field_names: List[str],
        target_schema: Dict[str, str],
    ) -> Dict[str, str]:
        """Fallback field mapping using simple heuristics."""
        mappings = {}
        
        # Common field name patterns
        patterns = {
            "date": ["date", "time", "timestamp", "created", "datetime"],
            "volume": ["volume", "vol", "amount", "quantity"],
            "price": ["price", "close", "value", "rate"],
        }
        
        field_names_lower = {f.lower(): f for f in field_names}
        
        for target, keywords in patterns.items():
            if target in target_schema:
                for kw in keywords:
                    for fn_lower, fn_orig in field_names_lower.items():
                        if kw in fn_lower:
                            mappings[target] = fn_orig
                            break
                    if target in mappings:
                        break
        
        return mappings

