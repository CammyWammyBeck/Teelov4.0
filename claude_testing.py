"""Investigation: Query all fields for match ID 669252."""

from sqlalchemy import select, text
from teelo.db.models import Match, MatchFeatures, FeatureSet, Player, Tournament, TournamentEdition
from teelo.db.session import get_session


def main():
    with get_session() as session:
        # Load the match with all relationships eagerly via a join query
        result = session.execute(
            select(Match)
            .where(Match.id == 669252)
        ).scalar_one_or_none()

        if result is None:
            print("Match 669252 not found in database.")
            return

        m = result

        print("=" * 70)
        print(f"MATCH ID: {m.id}")
        print("=" * 70)

        # Core identifiers
        print(f"\n-- Identifiers --")
        print(f"  external_id:          {m.external_id}")
        print(f"  source:               {m.source}")
        print(f"  status:               {m.status}")
        print(f"  temporal_order:       {m.temporal_order}")

        # Players
        print(f"\n-- Players --")
        pa = session.get(Player, m.player_a_id)
        pb = session.get(Player, m.player_b_id)
        print(f"  player_a_id:          {m.player_a_id}  -> {pa.canonical_name if pa else 'NOT FOUND'}")
        print(f"  player_b_id:          {m.player_b_id}  -> {pb.canonical_name if pb else 'NOT FOUND'}")
        print(f"  player_a_seed:        {m.player_a_seed}")
        print(f"  player_b_seed:        {m.player_b_seed}")
        if m.winner_id:
            pw = session.get(Player, m.winner_id)
            print(f"  winner_id:            {m.winner_id}  -> {pw.canonical_name if pw else 'NOT FOUND'}")
        else:
            print(f"  winner_id:            None")

        # Tournament context
        print(f"\n-- Tournament --")
        te = session.get(TournamentEdition, m.tournament_edition_id) if m.tournament_edition_id else None
        if te:
            t = session.get(Tournament, te.tournament_id)
            print(f"  tournament_edition_id:{m.tournament_edition_id}")
            print(f"  tournament name:      {t.name if t else 'NOT FOUND'}")
            print(f"  tour:                 {t.tour if t else 'N/A'}")
            print(f"  level:                {t.level if t else 'N/A'}")
            print(f"  surface (tour):       {t.surface if t else 'N/A'}")
            print(f"  surface (edition):    {te.surface}")
            print(f"  year:                 {te.year}")
            print(f"  start_date:           {te.start_date}")
            print(f"  end_date:             {te.end_date}")
        else:
            print(f"  tournament_edition_id:{m.tournament_edition_id}  (no record found)")
        print(f"  round:                {m.round}")
        print(f"  draw_position:        {m.draw_position}")
        print(f"  match_number:         {m.match_number}")

        # Scheduling
        print(f"\n-- Scheduling --")
        print(f"  scheduled_date:       {m.scheduled_date}")
        print(f"  scheduled_datetime:   {m.scheduled_datetime}")
        print(f"  court:                {m.court}")
        print(f"  match_date:           {m.match_date}")
        print(f"  match_date_estimated: {m.match_date_estimated}")
        print(f"  match_datetime:       {m.match_datetime}")
        print(f"  duration_minutes:     {m.duration_minutes}")

        # ELO
        print(f"\n-- ELO --")
        print(f"  elo_pre_player_a:     {m.elo_pre_player_a}")
        print(f"  elo_pre_player_b:     {m.elo_pre_player_b}")
        print(f"  elo_post_player_a:    {m.elo_post_player_a}")
        print(f"  elo_post_player_b:    {m.elo_post_player_b}")
        print(f"  elo_params_version:   {m.elo_params_version}")
        print(f"  elo_processed_at:     {m.elo_processed_at}")
        print(f"  elo_needs_recompute:  {m.elo_needs_recompute}")

        # Predictions
        print(f"\n-- Predictions --")
        print(f"  prediction_a:         {m.prediction_a}")
        print(f"  prediction_model_ver: {m.prediction_model_version}")
        print(f"  prediction_updated_at:{m.prediction_updated_at}")
        print(f"  prediction_source:    {m.prediction_source}")

        # Betting odds
        print(f"\n-- Betting Odds --")
        print(f"  odds_a:               {m.odds_a}")
        print(f"  odds_b:               {m.odds_b}")
        print(f"  odds_source:          {m.odds_source}")
        print(f"  odds_updated_at:      {m.odds_updated_at}")

        # Result
        print(f"\n-- Result --")
        print(f"  score:                {m.score}")
        print(f"  score_structured:     {m.score_structured}")
        print(f"  retirement_set:       {m.retirement_set}")
        print(f"  stats:                {m.stats}")

        # Metadata
        print(f"\n-- Metadata --")
        print(f"  created_at:           {m.created_at}")
        print(f"  updated_at:           {m.updated_at}")

        # Features
        print(f"\n-- Match Features --")
        features_rows = session.execute(
            select(MatchFeatures, FeatureSet)
            .join(FeatureSet, MatchFeatures.feature_set_id == FeatureSet.id)
            .where(MatchFeatures.match_id == m.id)
        ).all()

        if not features_rows:
            print("  (no match_features rows found for this match)")
        else:
            for mf, fs in features_rows:
                print(f"\n  FeatureSet: id={fs.id} name={fs.name} version={fs.version}")
                print(f"  match_features.id:    {mf.id}")
                print(f"  computed_at:          {mf.computed_at}")
                # Print features vector summary
                if mf.features is not None:
                    if isinstance(mf.features, list):
                        print(f"  features (list):      len={len(mf.features)}")
                        print(f"  first 10 values:      {mf.features[:10]}")
                    elif isinstance(mf.features, dict):
                        print(f"  features (dict):      {len(mf.features)} keys")
                        for k, v in mf.features.items():
                            print(f"    {k}: {v}")
                    else:
                        print(f"  features:             {type(mf.features)} = {str(mf.features)[:200]}")
                else:
                    print(f"  features:             None")

        print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
