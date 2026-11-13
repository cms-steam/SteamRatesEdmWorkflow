"""steamrates: HLT/L1 rate estimation for CMS STEAM.

The package replaces the counting step (step 2) of the legacy SteamRatesEdmWorkflow scripts.

Main idea: a counting job does not compute rates directly. It reads the trigger bits of every event
and stores each *distinct combination* of fired triggers once, together with how often it occurred
(see ``steamrates.patterns``). Rates, dataset/stream/group rates, pure rates and overlaps are all
derived from these "combination tables" at merge time.

Entry points (run from the repository root):
    python3 -m steamrates.count    ...   # ntuple files -> one partial result (ROOT file)
    python3 -m steamrates.summary  ...   # inspect / sum partial results
"""

__version__ = "0.1.0"
