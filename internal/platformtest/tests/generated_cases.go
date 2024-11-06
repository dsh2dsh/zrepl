// Code generated by zrepl tooling; DO NOT EDIT.

package tests

var Cases = []Case{BatchDestroy,
	CreateReplicationCursor,
	GetNonexistent,
	HoldsWork,
	IdempotentBookmark,
	IdempotentDestroy,
	IdempotentHold,
	ListFilesystemVersionsFilesystemNotExist,
	ListFilesystemVersionsTypeFilteringAndPrefix,
	ListFilesystemVersionsUserrefs,
	ListFilesystemVersionsZeroExistIsNotAnError,
	ListFilesystemsNoFilter,
	ReceiveForceIntoEncryptedErr,
	ReceiveForceRollbackWorksUnencrypted,
	ReplicationFailingInitialParentProhibitsChildReplication,
	ReplicationIncrementalCleansUpStaleAbstractionsWithCacheOnSecondReplication,
	ReplicationIncrementalCleansUpStaleAbstractionsWithoutCacheOnSecondReplication,
	ReplicationIncrementalDestroysStepHoldsIffIncrementalStepHoldsAreDisabledButStepHoldsExist,
	ReplicationIncrementalHandlesFromVersionEqTentativeCursorCorrectly,
	ReplicationIncrementalIsPossibleIfCommonSnapshotIsDestroyed,
	ReplicationInitialAll,
	ReplicationInitialFail,
	ReplicationInitialMostRecent,
	ReplicationIsResumableFullSend__both_GuaranteeResumability,
	ReplicationIsResumableFullSend__initial_GuaranteeIncrementalReplication_incremental_GuaranteeIncrementalReplication,
	ReplicationIsResumableFullSend__initial_GuaranteeResumability_incremental_GuaranteeIncrementalReplication,
	ReplicationOfPlaceholderFilesystemsInChainedReplicationScenario,
	ReplicationPlaceholderEncryption__EncryptOnReceiverUseCase__WorksIfConfiguredWithInherit,
	ReplicationPlaceholderEncryption__UnspecifiedIsOkForClientIdentityPlaceholder,
	ReplicationPlaceholderEncryption__UnspecifiedLeadsToFailureAtRuntimeWhenCreatingPlaceholders,
	ReplicationPropertyReplicationWorks,
	ReplicationReceiverErrorWhileStillSending,
	ReplicationStepCompletedLostBehavior__GuaranteeIncrementalReplication,
	ReplicationStepCompletedLostBehavior__GuaranteeResumability,
	ResumableRecvAndTokenHandling,
	ResumeTokenParsing,
	SendArgsValidationEE_EncryptionAndRaw,
	SendArgsValidationEncryptedSendOfUnencryptedDatasetForbidden__EncryptionSupported_false,
	SendArgsValidationEncryptedSendOfUnencryptedDatasetForbidden__EncryptionSupported_true,
	SendArgsValidationResumeTokenDifferentFilesystemForbidden,
	SendArgsValidationResumeTokenEncryptionMismatchForbidden,
	SendStreamCloseAfterBlockedOnPipeWrite,
	SendStreamCloseAfterEOFRead,
	SendStreamMultipleCloseAfterEOF,
	SendStreamMultipleCloseBeforeEOF,
	SendStreamNonEOFReadErrorHandling,
	UndestroyableSnapshotParsing,
}