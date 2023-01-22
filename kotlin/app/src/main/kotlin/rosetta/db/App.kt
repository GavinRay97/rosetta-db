/*
 * This Kotlin source file was generated by the Gradle 'init' task.
 */
package rosetta.db

import com.sun.nio.file.ExtendedOpenOption
import mu.KotlinLogging
import java.io.File
import java.lang.foreign.MemorySegment
import java.lang.foreign.MemorySession
import java.lang.foreign.ValueLayout
import java.nio.channels.FileChannel
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.ConcurrentHashMap

const val PAGE_SIZE = 4096
const val BUFFER_POOL_PAGES = 1000

object Constants {
    val JAVA_SHORT_UNALIGNED = ValueLayout.JAVA_SHORT.withBitAlignment(8)
    val JAVA_INT_UNALIGNED = ValueLayout.JAVA_INT.withBitAlignment(8)
    val JAVA_LONG_UNALIGNED = ValueLayout.JAVA_LONG.withBitAlignment(8)
}

data class TableLocation(
    val dbId: Int,
    val tableId: Int
)

data class PageLocation(
    val dbId: Int,
    val tableId: Int,
    val pageId: Int
)

data class RecordId(
    val pageId: Int,
    val slotId: Int
)

interface IDiskManager {
    fun getDatabaseDirectory(dbId: Int): Path
    fun createDatabaseDirectory(dbId: Int): Path
    fun removeDatabaseDirectory(dbId: Int): Boolean

    fun getTableFilePath(tableLocation: TableLocation): Path
    fun createTableFile(tableLocation: TableLocation): Path
    fun removeTableFile(tableLocation: TableLocation): Boolean
    fun openTableFile(tableLocation: TableLocation): FileChannel

    fun readPage(pageLocation: PageLocation, page: MemorySegment)
    fun writePage(pageLocation: PageLocation, page: MemorySegment)

    fun getNumPages(tableLocation: TableLocation): Int
    fun allocatePage(tableLocation: TableLocation): Int
}

object DiskManager : IDiskManager {
    // The root of the database directory
    const val DB_ROOT = "/tmp/mini-sql-engine"
    private val logger = KotlinLogging.logger {}

    private val fcCache = ConcurrentHashMap<TableLocation, FileChannel>()
    private val OPEN_OPTIONS: Set<OpenOption> =
        setOf(StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE, ExtendedOpenOption.DIRECT)


    override fun getDatabaseDirectory(dbId: Int): Path {
        return Path.of(DB_ROOT, "db_$dbId")
    }

    override fun getTableFilePath(tableLocation: TableLocation): Path {
        return Path.of(DB_ROOT, "db_${tableLocation.dbId}", "table_${tableLocation.tableId}")
    }


    // Gets the file for a table, creating the directory if it doesn't exist
    private fun getOrCreateTableFile(tableLocation: TableLocation): FileChannel {
        return fcCache.computeIfAbsent(tableLocation) {
            val tablePath = getTableFilePath(tableLocation)
            // Create the directory if it doesn't exist
            tablePath.parent.toFile().mkdirs()
            FileChannel.open(tablePath, OPEN_OPTIONS)
        }
    }

    override fun createDatabaseDirectory(dbId: Int): Path {
        logger.info { "Creating database directory for dbId $dbId" }
        val dbPath = getDatabaseDirectory(dbId)
        if (!dbPath.toFile().exists()) {
            dbPath.toFile().mkdirs()
        }
        return dbPath
    }

    override fun removeDatabaseDirectory(dbId: Int): Boolean {
        logger.info { "Removing database directory for dbId $dbId" }
        val dbPath = getDatabaseDirectory(dbId)
        return dbPath.toFile().deleteRecursively()
    }

    override fun createTableFile(tableLocation: TableLocation): Path {
        logger.info { "Creating table file for table $tableLocation" }
        val tablePath = getTableFilePath(tableLocation)
        if (!tablePath.toFile().exists()) {
            tablePath.toFile().createNewFile()
        }
        return tablePath
    }

    override fun removeTableFile(tableLocation: TableLocation): Boolean {
        logger.info { "Removing table file for table $tableLocation" }
        val tablePath = getTableFilePath(tableLocation)
        return tablePath.toFile().delete()
    }

    override fun openTableFile(tableLocation: TableLocation): FileChannel {
        logger.info { "Opening table file for table $tableLocation" }
        return getOrCreateTableFile(tableLocation)
    }

    override fun readPage(pageLocation: PageLocation, page: MemorySegment) {
        logger.info { "Reading page $pageLocation" }
        val fc = getOrCreateTableFile(TableLocation(pageLocation.dbId, pageLocation.tableId))
        val alignedBuffer = page.asByteBuffer().alignedSlice(PAGE_SIZE)  // Align to PAGE_SIZE for Direct I/O (O_DIRECT)
        val readBytes = fc.read(alignedBuffer, pageLocation.pageId * PAGE_SIZE.toLong())
        logger.info { "Read $readBytes bytes" }
        // Special case for empty files, fill the buffer with 0's
        if (readBytes == 0) {
            page.fill((0).toByte())
            return
        }
        if (readBytes != PAGE_SIZE) {
            throw IllegalStateException("Read $readBytes bytes, expected $PAGE_SIZE")
        }
    }

    override fun writePage(pageLocation: PageLocation, page: MemorySegment) {
        logger.info { "Writing page $pageLocation" }
        val fc = getOrCreateTableFile(TableLocation(pageLocation.dbId, pageLocation.tableId))
        val alignedBuffer = page.asByteBuffer().alignedSlice(PAGE_SIZE) // Align to PAGE_SIZE for Direct I/O (O_DIRECT)
        val writtenBytes = fc.write(alignedBuffer, pageLocation.pageId * PAGE_SIZE.toLong())
        logger.info { "Wrote $writtenBytes bytes" }
    }

    // TODO: Should this be named "getNumPagesZeroIndexed"?
    override fun getNumPages(tableLocation: TableLocation): Int {
        logger.info { "Getting number of pages for table $tableLocation" }
        val fc = getOrCreateTableFile(tableLocation)
        val numPages = (fc.size() / PAGE_SIZE).toInt()
        // TODO: This feels gross, revisit this if you get a better idea
        return when (numPages) {
            0 -> 0
            else -> numPages - 1 // Pages are 0-indexed
        }
    }

    override fun allocatePage(tableLocation: TableLocation): Int {
        logger.info { "Allocating page for table $tableLocation" }
        val fc = getOrCreateTableFile(tableLocation)
        val newPageId = getNumPages(tableLocation)
        val emptyBuffer = MemorySegment.allocateNative(PAGE_SIZE.toLong(), MemorySession.global())
        val alignedBuffer = emptyBuffer.asByteBuffer().alignedSlice(PAGE_SIZE)
        fc.write(alignedBuffer)
        return newPageId
    }
}

interface IBufferPool {
    fun fetchPage(pageLocation: PageLocation): MemorySegment
    fun unpinPage(pageLocation: PageLocation, isDirty: Boolean)
    fun flushPage(pageLocation: PageLocation)
    fun flushAllPages(dbId: Int)
}

class BufferPool : IBufferPool {
    private val logger = KotlinLogging.logger {}

    private val bufferPool: MemorySegment
    private val pages: Array<MemorySegment>
    private val freeList: ArrayDeque<Int>
    private val pinCount: IntArray
    private val isDirty: BooleanArray
    private val refBit: BooleanArray
    private val pageTable: ConcurrentHashMap<PageLocation, Int>

    init {
        bufferPool = MemorySegment.allocateNative(PAGE_SIZE * BUFFER_POOL_PAGES.toLong(), MemorySession.global())
        pages = Array(BUFFER_POOL_PAGES) { bufferPool.asSlice((it * PAGE_SIZE).toLong(), PAGE_SIZE.toLong()) }
        freeList = ArrayDeque((0 until BUFFER_POOL_PAGES).toList())

        isDirty = BooleanArray(BUFFER_POOL_PAGES)
        refBit = BooleanArray(BUFFER_POOL_PAGES)
        pinCount = IntArray(BUFFER_POOL_PAGES)

        pageTable = ConcurrentHashMap()
    }

    override fun fetchPage(pageLocation: PageLocation): MemorySegment {
        logger.info { "Fetching page $pageLocation" }
        val pageId = pageTable[pageLocation]
        if (pageId != null) {
            pinCount[pageId]++
            refBit[pageId] = true
            return pages[pageId]
        }

        val freePageId = freeList.removeFirst()
        DiskManager.readPage(pageLocation, pages[freePageId])
        pinCount[freePageId]++
        refBit[freePageId] = true
        pageTable[pageLocation] = freePageId
        return pages[freePageId]
    }

    override fun unpinPage(pageLocation: PageLocation, isDirty: Boolean) {
        logger.info { "Unpinning page $pageLocation" }
        val pageId = pageTable[pageLocation]!!
        pinCount[pageId]--
        this.isDirty[pageId] = isDirty
    }

    override fun flushPage(pageLocation: PageLocation) {
        logger.info { "Flushing page $pageLocation" }
        val pageId = pageTable[pageLocation]!!
        if (isDirty[pageId]) {
            DiskManager.writePage(pageLocation, pages[pageId])
            isDirty[pageId] = false
        }

        freeList.addLast(pageId)
        pageTable.remove(pageLocation)

        pinCount[pageId] = 0
        isDirty[pageId] = false
        refBit[pageId] = false
    }

    override fun flushAllPages(dbId: Int) {
        logger.info { "Flushing all pages for dbId $dbId" }
        pageTable.keys.filter { it.dbId == dbId }.forEach { flushPage(it) }
    }
}

// Accessor Methods for Heap Page memory layout
object HeapPage {
    private val logger = KotlinLogging.logger {}

    private const val PAGE_LSN_OFFSET = 0L
    private const val PAGE_ID_OFFSET = 8L
    private const val PAGE_NUM_SLOTS_OFFSET = 12L
    private const val PAGE_FREE_SPACE_OFFSET = 16L
    private const val PAGE_HEADER_SIZE = 20L
    private const val PAGE_SLOT_SIZE = 4L
    private const val PAGE_MAX_TUPLE_SIZE = PAGE_SIZE - PAGE_HEADER_SIZE - PAGE_SLOT_SIZE
    private const val DELETED_RECORD_FLAG = 0x8000.toShort()

    fun getLSN(page: MemorySegment): Long {
        return page.get(Constants.JAVA_LONG_UNALIGNED, PAGE_LSN_OFFSET)
    }

    fun setLSN(page: MemorySegment, lsn: Long) {
        page.set(Constants.JAVA_LONG_UNALIGNED, PAGE_LSN_OFFSET, lsn)
    }

    fun getPageId(page: MemorySegment): Int {
        return page.get(Constants.JAVA_INT_UNALIGNED, PAGE_ID_OFFSET)
    }

    fun setPageId(page: MemorySegment, pageId: Int) {
        page.set(Constants.JAVA_INT_UNALIGNED, PAGE_ID_OFFSET, pageId)
    }

    fun getNumSlots(page: MemorySegment): Short {
        return page.get(Constants.JAVA_SHORT_UNALIGNED, PAGE_NUM_SLOTS_OFFSET)
    }

    fun setNumSlots(page: MemorySegment, numSlots: Short) {
        page.set(Constants.JAVA_SHORT_UNALIGNED, PAGE_NUM_SLOTS_OFFSET, numSlots)
    }

    fun getFreeSpace(page: MemorySegment): Short {
        return page.get(Constants.JAVA_SHORT_UNALIGNED, PAGE_FREE_SPACE_OFFSET)
    }

    fun setFreeSpace(page: MemorySegment, freeSpace: Short) {
        page.set(Constants.JAVA_SHORT_UNALIGNED, PAGE_FREE_SPACE_OFFSET, freeSpace)
    }

    fun getSlotOffset(page: MemorySegment, slotId: Int): Short {
        return page.get(Constants.JAVA_SHORT_UNALIGNED, PAGE_HEADER_SIZE + slotId * PAGE_SLOT_SIZE)
    }

    fun setSlotOffset(page: MemorySegment, slotId: Short, offset: Short) {
        page.set(Constants.JAVA_SHORT_UNALIGNED, PAGE_HEADER_SIZE + slotId * PAGE_SLOT_SIZE, offset)
    }

    fun getSlotLength(page: MemorySegment, slotId: Int): Short {
        return page.get(Constants.JAVA_SHORT_UNALIGNED, PAGE_HEADER_SIZE + slotId * PAGE_SLOT_SIZE + 2)
    }

    fun setSlotLength(page: MemorySegment, slotId: Short, length: Short) {
        page.set(Constants.JAVA_SHORT_UNALIGNED, PAGE_HEADER_SIZE + slotId * PAGE_SLOT_SIZE + 2, length)
    }

    fun getTuple(page: MemorySegment, slotId: Int): MemorySegment {
        val offset = getSlotOffset(page, slotId)
        val length = getSlotLength(page, slotId)
        return page.asSlice(offset.toLong(), length.toLong())
    }

    fun insertTuple(page: MemorySegment, tuple: MemorySegment): RecordId {
        logger.info {
            "Inserting tuple: [page id=${getPageId(page)}, tuple size=${tuple.byteSize()}, " +
                    "free space=${getFreeSpace(page)}, num slots=${getNumSlots(page)}]"
        }

        val numSlots = getNumSlots(page)
        val freeSpace = getFreeSpace(page)
        val tupleSize = tuple.byteSize()
        if (tupleSize > PAGE_MAX_TUPLE_SIZE) {
            throw IllegalArgumentException("Tuple size is too large")
        }
        if (tupleSize > freeSpace) {
            throw IllegalArgumentException("Not enough space")
        }
        // Copy the tuple to the page
        val offset = freeSpace - tupleSize
        MemorySegment.copy(tuple, 0, page, offset, tupleSize)
        // Update the page header
        setSlotOffset(page, numSlots, offset.toShort())
        setSlotLength(page, numSlots, tupleSize.toShort())
        setNumSlots(page, (numSlots + 1).toShort())
        setFreeSpace(page, (freeSpace - tupleSize).toShort())
        return RecordId(getPageId(page), numSlots.toInt())
    }

    fun deleteTuple(page: MemorySegment, slotId: Short) {
        val freeSpace = getFreeSpace(page)
        val length = getSlotLength(page, slotId.toInt())
        // Update the page header
        setSlotOffset(page, slotId, DELETED_RECORD_FLAG)
        setSlotLength(page, slotId, 0)
        setFreeSpace(page, (freeSpace + length).toShort())
        // We clean up deleted tuples lazily, so we don't need to do anything here
    }

    fun isDeleted(page: MemorySegment, slotId: Short): Boolean {
        return getSlotOffset(page, slotId.toInt()) == DELETED_RECORD_FLAG
    }

    fun getRecordId(page: MemorySegment, slotId: Int): RecordId {
        return RecordId(getPageId(page), slotId)
    }

    fun initializeEmptyPage(page: MemorySegment, pageId: Int) {
        setLSN(page, 0)
        setPageId(page, pageId)
        setNumSlots(page, 0)
        setFreeSpace(page, PAGE_SIZE.toShort())
    }
}

class HeapFile(
    val bufferPool: IBufferPool,
    val diskManager: IDiskManager,
    val tableLocation: TableLocation,
) {
    private var numPages: Int

    private val logger = KotlinLogging.logger {}

    init {
        logger.info { "Initializing HeapFile class for table $tableLocation" }
        numPages = diskManager.getNumPages(tableLocation)

        logger.info { "Found $numPages pages in file" }
        if (numPages == 0) {
            logger.info { "Table $tableLocation is empty, creating a new page" }
            val location = getPageLocation(0)
            val page = bufferPool.fetchPage(location)
            HeapPage.initializeEmptyPage(page, 0)
            bufferPool.unpinPage(location, true)
        }
    }

    private fun getPageLocation(pageId: Int): PageLocation {
        return PageLocation(tableLocation.dbId, tableLocation.tableId, pageId)
    }

    fun deleteFile() {
        diskManager.removeTableFile(tableLocation)
    }

    fun getNumPages(): Int {
        return numPages
    }

    fun getFreeSpace(pageId: Int): Int {
        val location = getPageLocation(pageId)
        val page = bufferPool.fetchPage(location)
        val freeSpace = HeapPage.getFreeSpace(page)
        bufferPool.unpinPage(location, false)
        return freeSpace.toInt()
    }

    fun getTuple(recordId: RecordId): MemorySegment {
        val location = getPageLocation(recordId.pageId)
        val page = bufferPool.fetchPage(location)
        val tuple = HeapPage.getTuple(page, recordId.slotId)
        bufferPool.unpinPage(location, false)
        return tuple
    }

    fun insertTuple(tuple: MemorySegment): RecordId {
        logger.info { "Inserting tuple $tuple into table $tableLocation" }
        val location = getPageLocation(numPages)
        val page = bufferPool.fetchPage(location)
        // Try to insert the tuple into the page
        // Catching the exception means that the page is full, so we need to create a new page
        return try {
            val recordId = HeapPage.insertTuple(page, tuple)
            bufferPool.unpinPage(location, isDirty = true)
            recordId
        } catch (e: IllegalArgumentException) {
            bufferPool.unpinPage(location, isDirty = false)
            logger.info { "Page $location is full, creating a new page" }
            val newLocation = getPageLocation(numPages)
            val newPage = bufferPool.fetchPage(newLocation)
            HeapPage.initializeEmptyPage(newPage, numPages)
            bufferPool.unpinPage(newLocation, isDirty = true)
            numPages++
            insertTuple(tuple)
        }
    }

    fun deleteTuple(recordId: RecordId) {
        val location = getPageLocation(recordId.pageId)
        val page = bufferPool.fetchPage(location)
        HeapPage.deleteTuple(page, recordId.slotId.toShort())
        bufferPool.unpinPage(location, isDirty = true)
    }

    fun flushAllPages() {
        bufferPool.flushAllPages(dbId = tableLocation.dbId)
    }

    fun <T> scan(
        callback: (RecordId, MemorySegment) -> T,
    ) {
        logger.info { "Scanning table $tableLocation with total pages: $numPages" }
        for (pageId in 0 until numPages + 1) {
            logger.info { "Scanning page $pageId" }
            val location = getPageLocation(pageId)
            val page = bufferPool.fetchPage(location)
            val numSlots = HeapPage.getNumSlots(page)
            for (slotId in 0 until numSlots) {
                if (!HeapPage.isDeleted(page, slotId.toShort())) {
                    val recordId = HeapPage.getRecordId(page, slotId)
                    val tuple = HeapPage.getTuple(page, slotId)
                    callback(recordId, tuple)
                }
            }
            bufferPool.unpinPage(location, isDirty = false)
        }
    }
}

fun bufferPoolTest() {
    val bufferPool = BufferPool()
    val pageLocation = PageLocation(1, 2, 3)

    val page = bufferPool.fetchPage(pageLocation)
    page.asByteBuffer().putInt(0, 42)

    bufferPool.unpinPage(pageLocation, true)
    bufferPool.flushPage(pageLocation)

    val page2 = bufferPool.fetchPage(pageLocation)
    println(page2.asByteBuffer().int)
}

fun heapPageTest() {
    val bufferPool = BufferPool()
    val pageLocation = PageLocation(1, 2, 3)

    val page = bufferPool.fetchPage(pageLocation)
    // Initialize the Heap Page free space
    HeapPage.setFreeSpace(page, PAGE_SIZE.toShort())

    // Insert a tuple
    val tuple = MemorySegment.allocateNative(4, MemorySession.global())
    tuple.asByteBuffer().putInt(0, 42)
    val recordId = HeapPage.insertTuple(page, tuple)
    bufferPool.unpinPage(pageLocation, true)

    // Get the tuple
    val page3 = bufferPool.fetchPage(pageLocation)
    val tuple2 = HeapPage.getTuple(page3, recordId.slotId)
    println("Got tuple: ${tuple2.asByteBuffer().int}")
}

fun heapFileTest() {
    val bufferPool = BufferPool()

    val tuple = MemorySegment.allocateNative(4, MemorySession.global())
    tuple.asByteBuffer().putInt(0, 42)

    // Wipe the current DB_ROOT
    val dbRoot = File(DiskManager.DB_ROOT)
    dbRoot.deleteRecursively()

    // Create a HeapFile
    val heapFile = HeapFile(bufferPool, DiskManager, TableLocation(0, 0))
    assert(heapFile.getNumPages() == 1)
    assert(heapFile.getFreeSpace(0) == PAGE_SIZE)

    val recordId1 = heapFile.insertTuple(tuple)
    val recordId2 = heapFile.insertTuple(tuple)
    println("Inserted tuple at $recordId1")
    println("Inserted tuple at $recordId2")

    // Scan the HeapFile
    heapFile.scan { recordId, tuple ->
        println("Scanned tuple: ${tuple.asByteBuffer().int}")
    }

    heapFile.flushAllPages()
}

fun main() {
    heapFileTest()
}

