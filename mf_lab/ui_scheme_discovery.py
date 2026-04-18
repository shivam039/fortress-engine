"""
MF Scheme Browser & Discovery UI
- Browse all 4000+ schemes by category
- View categorization stats (from pre-computed batches)
- Select schemes for detailed analysis
- **OPTIMIZED:** Uses pre-batched database queries for instant filtering
"""

import streamlit as st
import pandas as pd
from mf_lab.services.scheme_discovery import (
    get_all_schemes_cached,
    get_schemes_by_category,
    get_category_stats,
    get_schemes_summary,
    get_batch_stats,
    get_batch_filtered_schemes,
    get_distinct_fund_types,
    get_distinct_categories_for_type,
    SCHEME_CATEGORIES,
)


def render_scheme_discovery_tab():
    """
    Render the scheme discovery and browsing interface.
    """
    st.header("🔍 Mutual Fund Scheme Browser")

    st.markdown("""
    Explore all **4000+ mutual fund schemes** available in India.
    Schemes are automatically categorized and cached monthly for optimal performance.
    """)

    # 1. Load all schemes (cached for 30 days)
    with st.spinner("Loading scheme catalog..."):
        all_schemes = get_all_schemes_cached()

    if all_schemes.empty:
        st.error("❌ Failed to load scheme catalog. Please try again later.")
        return

    # 2. Summary statistics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("📊 Total Schemes", f"{len(all_schemes):,}")

    with col2:
        st.metric("🏢 Active AMCs", all_schemes["amc_name"].nunique())

    with col3:
        eq_count = len(all_schemes[all_schemes["type"] == "Equity"])
        st.metric("📈 Equity Schemes", eq_count)

    with col4:
        debt_count = len(all_schemes[all_schemes["type"] == "Debt"])
        st.metric("💰 Debt Schemes", debt_count)

    st.divider()

    # 3. Category browser tabs
    tab_stats, tab_by_type, tab_by_category, tab_search = st.tabs(
        ["Summary", "By Type", "By Category", "Search"]
    )

    # TAB 1: SUMMARY STATISTICS
    with tab_stats:
        st.subheader("Scheme Distribution Overview")

        stats = get_category_stats()
        summary = get_schemes_summary()

        # Type breakdown
        col1, col2 = st.columns([1, 1])

        with col1:
            st.write("**Schemes by Type:**")
            type_df = pd.Series(stats.get("by_type", {})).reset_index()
            type_df.columns = ["Type", "Count"]
            st.bar_chart(type_df.set_index("Type"))

        with col2:
            st.write("**Detailed Breakdown:**")
            breakdown_list = []
            for item in stats.get("by_category", []):
                breakdown_list.append({
                    "Type": item["type"],
                    "Category": item["category"],
                    "Schemes": item["scheme_count"],
                    "AMCs": item["amc_count"],
                })
            breakdown_df = pd.DataFrame(breakdown_list)
            st.dataframe(
                breakdown_df,
                width='stretch',
                hide_index=True,
                column_config={
                    "Type": st.column_config.TextColumn("Type"),
                    "Category": st.column_config.TextColumn("Category"),
                    "Schemes": st.column_config.NumberColumn("# Schemes"),
                    "AMCs": st.column_config.NumberColumn("# AMCs"),
                }
            )

    # TAB 2: BY TYPE (EQUITY / DEBT / HYBRID) — OPTIMIZED with pre-computed batches
    with tab_by_type:
        st.subheader("Browse by Fund Type")

        # ✨ INSTANT: Get types from pre-computed batch table (NOT from all_schemes)
        scheme_types = get_distinct_fund_types()
        
        if not scheme_types:
            st.warning("No fund types available. Please refresh cache.")
        else:
            selected_type = st.selectbox("Select Fund Type:", scheme_types)

            # ✨ OPTIMIZED: Query uses database index instead of in-memory filtering
            type_schemes = get_batch_filtered_schemes(scheme_type=selected_type)

            col1, col2 = st.columns([2, 1])

            with col1:
                st.write(f"**{len(type_schemes)} {selected_type} Schemes** (from pre-computed batch)")

            with col2:
                display_format = st.radio("Display:", ["Table", "List"], horizontal=True, key="type_format")

            if display_format == "Table":
                st.dataframe(
                    type_schemes.sort_values("scheme_name"),
                    width='stretch',
                    hide_index=True,
                    column_config={
                        "scheme_name": st.column_config.TextColumn("Scheme Name"),
                        "category": st.column_config.TextColumn("Category"),
                        "amc_name": st.column_config.TextColumn("AMC"),
                        "scheme_code": st.column_config.NumberColumn("Code"),
                    }
                )
            else:
                for _, row in type_schemes.sort_values("scheme_name").iterrows():
                    with st.container(border=True):
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.write(f"**{row['scheme_name']}**")
                            st.caption(f"{row['amc_name']} • {row['category']}")
                        with col2:
                            st.code(row['scheme_code'], language=None)

    # TAB 3: BY CATEGORY (DETAILED) — OPTIMIZED with pre-computed batches
    with tab_by_category:
        st.subheader("Browse by Category")

        # ✨ INSTANT: Get types from pre-computed batch table (NOT from all_schemes)
        available_types = get_distinct_fund_types()
        
        if not available_types:
            st.warning("No fund types available. Please refresh cache.")
        else:
            # First select fund type
            col1, col2 = st.columns(2)
            with col1:
                selected_type = st.selectbox("Select Fund Type:", available_types, key="cat_type_selector")
            
            # Then get categories for that type
            categories = get_distinct_categories_for_type(selected_type)
            
            with col2:
                if categories:
                    selected_category = st.selectbox("Select Category:", categories)
                else:
                    st.warning(f"No categories found for {selected_type}")
                    selected_category = None

            if selected_category:
                # ✨ OPTIMIZED: Query uses database index instead of in-memory filtering
                cat_schemes = get_batch_filtered_schemes(
                    scheme_type=selected_type, 
                    category=selected_category
                )

                if cat_schemes.empty:
                    st.info(f"No schemes found for {selected_type} → {selected_category}")
                else:
                    col1, col2, col3 = st.columns(3)

                    with col1:
                        st.metric("Schemes", len(cat_schemes))

                    with col2:
                        st.metric("AMCs", cat_schemes["amc_name"].nunique())

                    with col3:
                        st.metric("Type", selected_type)

                    st.divider()

                    # Show schemes table
                    display_cols = ["scheme_name", "amc_name", "scheme_code", "category"]
                    st.dataframe(
                        cat_schemes[display_cols].sort_values("scheme_name"),
                        width='stretch',
                        hide_index=True,
                        column_config={
                            "scheme_name": st.column_config.TextColumn("Scheme Name", width="large"),
                            "amc_name": st.column_config.TextColumn("AMC", width="medium"),
                            "scheme_code": st.column_config.NumberColumn("Code", width="small"),
                            "category": st.column_config.TextColumn("Category", width="small"),
                        }
                    )

                    # Export option
                    if st.button(f"📥 Export {selected_category} schemes to CSV"):
                        csv = cat_schemes.to_csv(index=False)
                        st.download_button(
                            label="Download CSV",
                            data=csv,
                            file_name=f"MF_{selected_category}.csv",
                            mime="text/csv",
                        )

    # TAB 4: SEARCH
    with tab_search:
        st.subheader("🔎 Search Schemes")

        search_query = st.text_input("Search by scheme name, AMC, or code:", placeholder="e.g., 'Parag Parikh', 'Nifty 50', '100100'")

        if search_query:
            query_lower = search_query.lower()

            results = all_schemes[
                (all_schemes["scheme_name"].str.lower().str.contains(query_lower, na=False)) |
                (all_schemes["amc_name"].str.lower().str.contains(query_lower, na=False)) |
                (all_schemes["scheme_code"].astype(str).str.contains(query_lower, na=False))
            ]

            if results.empty:
                st.warning(f"No schemes found matching '{search_query}'")
            else:
                st.success(f"Found {len(results)} matching schemes")

                st.dataframe(
                    results.sort_values("scheme_name"),
                    width='stretch',
                    hide_index=True,
                    column_config={
                        "scheme_name": st.column_config.TextColumn("Scheme Name", width="large"),
                        "category": st.column_config.TextColumn("Category", width="medium"),
                        "type": st.column_config.TextColumn("Type", width="small"),
                        "amc_name": st.column_config.TextColumn("AMC", width="medium"),
                        "scheme_code": st.column_config.NumberColumn("Code", width="small"),
                    }
                )

                # Multi-select for analysis
                st.divider()
                st.subheader("📊 Analyze Selected Schemes")

                selected_schemes = st.multiselect(
                    "Select schemes for analysis:",
                    options=results.index,
                    format_func=lambda x: f"{results.loc[x, 'scheme_name']} ({results.loc[x, 'scheme_code']})"
                )

                if selected_schemes and st.button("Run Analysis on Selected Schemes"):
                    selected_data = results.loc[selected_schemes, ["scheme_code", "scheme_name"]]
                    st.session_state["selected_schemes_for_analysis"] = selected_data.to_dict("records")
                    st.success(f"✅ Selected {len(selected_schemes)} schemes for analysis")
                    st.info("Go to the 'Analysis' tab to view detailed metrics")


def render_selected_schemes_analysis():
    """
    Render analysis results for user-selected schemes.
    Called from main UI after user selects schemes.
    """
    if "selected_schemes_for_analysis" not in st.session_state:
        st.info("Select schemes from the Scheme Browser to analyze them here")
        return

    schemes = st.session_state["selected_schemes_for_analysis"]
    scheme_codes = [s["scheme_code"] for s in schemes]

    st.subheader(f"📈 Analysis: {len(schemes)} Selected Schemes")

    # TODO: Call scoring functions from mf_lab.logic
    # This would parallel-fetch NAV data and compute metrics
    st.info("Analysis feature coming soon - will fetch and score selected schemes")
