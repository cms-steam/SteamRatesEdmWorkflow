import FWCore.ParameterSet.Config as cms
from CondCore.CondDB.CondDB_cfi import CondDB as _CondDB

def applyNewJEC(process):
    process.pfhcESSource = cms.ESSource('PoolDBESSource',
                                        _CondDB.clone(connect = 'sqlite_file:/eos/cms/store/group/phys_jetmet/saparede/runIII_hlt_jec/jescs_jul22/PFHC_Run3Summer21_MC.db'),
                                            
                                        
                                        toGet = cms.VPSet(
                                            cms.PSet(
                                                record = cms.string('PFCalibrationRcd'),
                                                tag = cms.string('PFCalibration_CMSSW_12_4_0_pre3_HLT_112X_mcRun3_2022'),
                                                label = cms.untracked.string('HLT'),
                                            ),
                                                    ),
                                          )
    process.pfhcESPrefer = cms.ESPrefer('PoolDBESSource', 'pfhcESSource')
    #process.hltParticleFlow.calibrationsLabel = '' # standard label for Offline-PFHC in GT

        ## ES modules for HLT JECs
    process.jescESSource = cms.ESSource('PoolDBESSource',
                                        _CondDB.clone(connect = 'sqlite_file:/eos/cms/store/group/phys_jetmet/saparede/runIII_hlt_jec/jescs_jul22/JESC_Run3Summer21_MC.db'),
                                         toGet = cms.VPSet(
                                             cms.PSet(
                                                 record = cms.string('JetCorrectionsRecord'),
                                                 tag = cms.string('JetCorrectorParametersCollection_Run3Summer21_MC_AK4CaloHLT'),
                                                 label = cms.untracked.string('AK4CaloHLT'),
                                             ),
                                             cms.PSet(
                                                 record = cms.string('JetCorrectionsRecord'),
                                                 tag = cms.string('JetCorrectorParametersCollection_Run3Summer21_MC_AK4PFClusterHLT'),
                                                 label = cms.untracked.string('AK4PFClusterHLT'),
                                             ),
                                             cms.PSet(
                                                 record = cms.string('JetCorrectionsRecord'),
                                                 tag = cms.string('JetCorrectorParametersCollection_Run3Summer21_MC_AK4PFHLT'),
                                                 label = cms.untracked.string('AK4PFHLT'),
                                             ),
                                             cms.PSet(
                                                 record = cms.string('JetCorrectionsRecord'),
                                                 tag = cms.string('JetCorrectorParametersCollection_Run3Summer21_MC_AK4PFchsHLT'),
                                                 label = cms.untracked.string('AK4PFchsHLT'),
                                             ),
                                             cms.PSet(
                                                 record = cms.string('JetCorrectionsRecord'),
                                                 tag = cms.string('JetCorrectorParametersCollection_Run3Summer21_MC_AK4PFPuppiHLT'),
                                                 label = cms.untracked.string('AK4PFPuppiHLT'),
                                             ),
                                             cms.PSet(
                                                 record = cms.string('JetCorrectionsRecord'),
                                                 tag = cms.string('JetCorrectorParametersCollection_Run3Summer21_MC_AK8CaloHLT'),
                                                 label = cms.untracked.string('AK8CaloHLT'),
                                             ),
                                             cms.PSet(
                                                 record = cms.string('JetCorrectionsRecord'),
                                                 tag = cms.string('JetCorrectorParametersCollection_Run3Summer21_MC_AK8PFClusterHLT'),
                                                 label = cms.untracked.string('AK8PFClusterHLT'),
                                             ),
                                             cms.PSet(
                                                 record = cms.string('JetCorrectionsRecord'),
                                                 tag = cms.string('JetCorrectorParametersCollection_Run3Summer21_MC_AK8PFHLT'),
                                                 label = cms.untracked.string('AK8PFHLT'),
                                             ),
                                             cms.PSet(
                                                 record = cms.string('JetCorrectionsRecord'),
                                                 tag = cms.string('JetCorrectorParametersCollection_Run3Summer21_MC_AK8PFchsHLT'),
                                                 label = cms.untracked.string('AK8PFchsHLT'),
                                             ),
                                             cms.PSet(
                                                 record = cms.string('JetCorrectionsRecord'),
                                                 tag = cms.string('JetCorrectorParametersCollection_Run3Summer21_MC_AK8PFPuppiHLT'),
                                                 label = cms.untracked.string('AK8PFPuppiHLT'),
                                             ),
                                         ),
    )
    process.jescESPrefer = cms.ESPrefer('PoolDBESSource', 'jescESSource')
    #Update HBHE thresholds
    process.hltParticleFlowRecHitHBHE.producers[0].qualityTests[0].name = "PFRecHitQTestHCALThresholdVsDepth"
    del process.hltParticleFlowRecHitHBHE.producers[0].qualityTests[0].threshold
    return process
