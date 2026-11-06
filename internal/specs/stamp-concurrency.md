# VDT-898: sequential SDK writes

Forwarder writes acquire process-local ownership by endpoint and user address
before refreshing signature data and nonce, signing, executing and processing the
receipt. Every instance refreshes its nonce inside ownership; cached nonces cannot
survive another instance's write. Direct Web3 writes use the same per-user queue
through receipt processing. Async dataset workers and batch helpers inherit these
boundaries. A shared confirmed next-nonce floor protects against signature-data
reads that lag a successful receipt. Reads and different users remain concurrent. Exceptions release the
queue; unknown outcomes are not retried automatically.

This is process-local coordination. It does not serialize another machine,
Lambda, process, or an app operation through VDT-896. The forwarder currently
accepts pre-signed requests without a shared user guard. Full rollout requires
routing shared-account writers through the guarded app API or a separately
reviewed forwarder nonce/ownership protocol. Do not claim distributed exclusion
from this SDK change or blindly replay an ambiguous execute response.

Ownership includes public receipt decoding and timestamped test-contract writes.
Direct clear operations also wait for their receipt before releasing the user, so
ordinary and test-subclass mutations cannot overlap pending nonce use.
