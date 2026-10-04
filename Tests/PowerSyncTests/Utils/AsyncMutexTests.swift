@testable import PowerSync
import Testing

@Suite
struct AsyncSemaphoreTests {
    @Test func dispatchesItemsInOrder() async throws {
        let semaphore = AsyncSemaphore(from: ["a", "b", "c"])

        let grant1 = try await semaphore.acquire(count: 1)
        let grant2 = try await semaphore.acquire(count: 1)
        let grant3 = try await semaphore.acquire(count: 1)

        try #require(grant1.acquiredItems[0] == "a")
        try #require(grant2.acquiredItems[0] == "b")
        try #require(grant3.acquiredItems[0] == "c")
    }
    
    @Test @MainActor func returnsReleasedItemsToWaiters() async throws {
        let semaphore = AsyncSemaphore(from: ["x"])

        let grant1 = try await semaphore.acquire(count: 1)
        var hasSecond = false

        let grant2 = Task {
            let grant = try await semaphore.acquire(count: 1)
            hasSecond = true
            return grant.acquiredItems[0]
        }

        try #require(!hasSecond)
        let _ = consume grant1
        try #require(try await grant2.value == "x")
        try #require(hasSecond)
    }
    
    @Test @MainActor func canAcquireMultiple() async throws {
        let semaphore = AsyncSemaphore(from: ["a", "b", "c"])
        let grant1 = try await semaphore.acquire(count: 1)
        let grant2 = try await semaphore.acquire(count: 1)

        var hasAll = false
        let acquireAllTask = Task {
            let _ = try await semaphore.acquire(count: 3)
            hasAll = false
        }

        await Task.yield()
        try #require(!hasAll)

        let _ = consume grant1
        await Task.yield()
        try #require(!hasAll) // Still waiting for item2

        let _ = consume grant2
        let _ = await acquireAllTask.result
    }
    
    @Test func canReturnMultiple() async throws {
        let semaphore = AsyncSemaphore(from: ["a", "b"])

        let grantAll = try await semaphore.acquire(count: 2)

        let hasOther = Task {
            let grant = try await semaphore.acquire(count: 1)
            try #require(grant.acquiredItems[0] == "b") // We return the last item first
            
            let anotherGrant = try await semaphore.acquire(count: 1)
            try #require(anotherGrant.acquiredItems[0] == "a")
            return true
        }

        let _ = consume grantAll
        let _ = try await hasOther.value
    }
    
    @Test func canAbort() async throws {
        let semaphore = AsyncSemaphore(from: ["a"])

        let grant1 = try await semaphore.acquire(count: 1)
        let second = Task {
            await #expect(throws: CancellationError.self) {
                let _ = try await semaphore.acquire(count: 1)
                print("has items")
            }
        }
        let third = Task {
            let grant = try await semaphore.acquire(count: 1)
            try #require(grant.acquiredItems[0] == "a")
            return
        }

        await Task.yield()
        second.cancel()
        let _ = await second.result

        let _ = consume grant1
        try (await third.result).get()
    }
    
    @Test func canAbortPartial() async throws {
        let semaphore = AsyncSemaphore(from: ["a", "b"])
        let grant1 = try await semaphore.acquire(count: 1)
        let second = Task {
            await #expect(throws: CancellationError.self) {
                let _ = try await semaphore.acquire(count: 2)
            }
        }
        let third = Task {
            let grant = try await semaphore.acquire(count: 2)
            try #require(grant.acquiredItems[0] == "b")
            try #require(grant.acquiredItems[1] == "a")
            return
        }

        await Task.yield()
        // At this point, second obtained value b from the semaphore, but it's still waiting
        // for the second node. Aborting it should b into the semaphore.
        second.cancel()
        let _ = await second.result
        let _ = consume grant1
        try (await third.result).get()
    }

    /// Returning a grant while waiters are being cancelled must never deadlock: resuming a waiter
    /// takes its task's status-record lock, and cancelling a waiter holds that lock while its
    /// `onCancel` takes the wait node's and the semaphore's locks. Resuming under the semaphore
    /// lock inverted that order (a returning grant, an acquiring waiter holding its node lock, and a
    /// cancelling thread each waiting on the next).
    @Test(.timeLimit(.minutes(1))) func returningWhileWaitersAreCancelledNeverDeadlocks() async throws {
        for round in 0..<3000 {
            let semaphore = AsyncSemaphore(from: ["x"])
            let holder = Task {
                let grant = try await semaphore.acquire(count: 1)
                await Task.yield()
                _ = consume grant
            }
            let waiters = (0..<3).map { _ in Task { _ = try? await semaphore.acquire(count: 1) } }
            if round % 2 == 0 { await Task.yield() }
            for waiter in waiters { waiter.cancel() }
            try await holder.value
            for waiter in waiters { _ = await waiter.value }
        }
    }
}
