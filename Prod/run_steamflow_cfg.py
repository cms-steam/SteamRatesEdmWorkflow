# Script to run the HLT menu saved in hlt_config.py, with STEAM customizations
# The main customization is creating an output file saving only information on which triggers were fired 
# Import HLT configuration #
from hlt_config import *

# STEAM Customization #

# Options
nEvents=-1             # number of events to process
switchL1PS=False       # apply L1 PS ratios to switch to tighter column
columnL1PS=1           # choose the tighter column ( 0 <=> tightest )
outputName="hlt.root"  # output file name (DO NOT CHANGE FROM "hlt.root", hardcoded elsewhere)

# Import input files
from list_cff import inputFileNames
process.source = cms.Source("PoolSource",
    fileNames = cms.untracked.vstring(inputFileNames),
    inputCommands = cms.untracked.vstring('keep *')
)

#Set max number of events
process.maxEvents = cms.untracked.PSet(
    input = cms.untracked.int32( nEvents )
)


# L1 customizations: change the L1 PS column to a tighter one
from HLTrigger.Configuration.common import *
import itertools

def insert_modules_after(process, target, *modules):
    "Add the `modules` after the `target` in any Sequence, Paths or EndPath that contains the latter."                                                      
    for sequence in itertools.chain(
        process._Process__sequences.itervalues(),
        process._Process__paths.itervalues(),
        process._Process__endpaths.itervalues()
    ):                                                                                                                                                      
        try:
            position = sequence.index ( target )
        except ValueError:
            continue
        else:
            for module in reversed(modules):
                sequence.insert(position+1, module)

process.l1tGlobalPrescaler = cms.EDFilter('L1TGlobalPrescaler',
  l1tResults = cms.InputTag('hltGtStage2Digis'),
  mode = cms.string('applyColumnRatios'),
  l1tPrescaleColumn = cms.uint32(columnL1PS)
)                                                                                                                                                           

if switchL1PS:
    insert_modules_after(process, process.hltGtStage2Digis, process.l1tGlobalPrescaler )
                                                                                                                                                            
    for module in filters_by_type(process, 'HLTL1TSeed'):
        module.L1GlobalInputTag = 'l1tGlobalPrescaler'

    for module in filters_by_type(process, 'HLTPrescaler'):
        module.L1GtReadoutRecordTag = 'l1tGlobalPrescaler'

#END L1 customizations


# Output file
process.DQMOutput.remove(process.dqmOutput)

process.hltOutput = cms.OutputModule( "PoolOutputModule",
     fileName = cms.untracked.string( outputName ),     #Set name of output ROOT file
     fastCloning = cms.untracked.bool( False ),
     dataset = cms.untracked.PSet(
         filterName = cms.untracked.string( "" ),
         dataTier = cms.untracked.string( "RAW" )
     ),
     outputCommands = cms.untracked.vstring( 'drop *',  #Drop all information...
         'keep edmTriggerResults_*_*_MYHLT',            #...EXCEPT info on which triggers were fired
         )
     )

process.HLTOutput = cms.EndPath( process.hltOutput )

