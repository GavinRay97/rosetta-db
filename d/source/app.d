import core.atomic;
import core.stdc.string;
import core.sys.linux.sys.mman;
import core.sys.posix.fcntl;
import core.sys.posix.unistd;
import core.sync.rwmutex;

import std;
import std.experimental.logger;

////////////////////////////////////////////
// CONSTANTS
////////////////////////////////////////////

enum PAGE_SIZE = 4096;
enum BUF_POOL_NUM_PAGES = 1000;

////////////////////////////////////////////
// TYPEDEFS
////////////////////////////////////////////

alias Page = ubyte[PAGE_SIZE]; // A view of the bytes in a page

alias lsn_t = uint64_t; // A log sequence number
alias frame_idx_t = size_t; // A frame index

alias db_id_t = uint32_t; // A database ID
alias table_id_t = uint32_t; // A table ID
alias page_num_t = uint32_t; // A page number
alias slot_num_t = uint32_t; // A slot number

////////////////////////////////////////////
// HELPER FUNCTIONS
////////////////////////////////////////////

struct TableLocation
{
	db_id_t db_id;
	table_id_t table_id;
}

// Equivalent to "RelFileLocator" in PostgreSQL, where "page_num" is the "block number"
struct PageLocation
{
	db_id_t db_id;
	table_id_t table_id;
	page_num_t page_num;
}

struct RecordID
{
	page_num_t page_num;
	slot_num_t slot_num;
}

////////////////////////////////////////////
// HEAP PAGE LAYOUT
////////////////////////////////////////////

enum HEAP_PAGE_MAX_SLOTS = (PAGE_SIZE - HeapPageHeader.sizeof) / HeapPageSlot.sizeof;

struct HeapPageHeader
{
	lsn_t lsn;
	page_num_t page_id;
	uint32_t num_slots;
	uint32_t free_space;
	uint32_t num_records;
}

struct HeapPageSlot
{
	uint16_t offset;
	uint32_t length;
}

align(PAGE_SIZE) union HeapPage
{
	ubyte[PAGE_SIZE] data;
	struct
	{
		HeapPageHeader header;
		HeapPageSlot[HEAP_PAGE_MAX_SLOTS] slots;
	}

	// Tombstone marker for deleted records
	static ushort TOMBSTONE = 0xFFFF;

	size_t get_free_space()
	{
		return header.free_space - HeapPageHeader.sizeof - HeapPageSlot.sizeof * header.num_slots;
	}

	ubyte[] get_record(slot_num_t slot_num)
	{
		infof("Getting record %d", slot_num);
		assert(slot_num < header.num_slots);
		auto slot = slots[slot_num];
		infof("Record offset: %d", slot.offset);
		if (slot.offset == TOMBSTONE)
		{
			infof("Record is deleted");
			return null;
		}
		return data[slot.offset .. slot.offset + slot.length];
	}

	RecordID insert_record(ubyte[] record)
	{
		infof("Inserting record of length %d", record.length);
		assert(record.length <= get_free_space(), "Record is too large to fit on page");
		slot_num_t slot_num = header.num_slots++;
		// Get a pointer to the slot
		auto slot = &slots[slot_num];
		slot.offset = cast(ushort)(header.free_space - record.length);
		slot.length = cast(uint) record.length;
		infof("Record offset: %d", slot.offset);
		infof("Record length: %d", slot.length);
		memcpy(data.ptr + slot.offset, record.ptr, record.length);
		header.free_space -= record.length;
		header.num_records++;
		return RecordID(header.page_id, slot_num);
	}

	void delete_record(slot_num_t slot_num)
	{
		assert(slot_num < header.num_slots);
		auto slot = &slots[slot_num];
		slot.offset = TOMBSTONE;
		header.free_space += slot.length;
		header.num_records--;
	}

	void inititalize(page_num_t page_id)
	{
		header.lsn = 0;
		header.page_id = page_id;
		header.num_slots = 0;
		header.free_space = PAGE_SIZE;
		header.num_records = 0;
	}
}

static assert(HeapPage.sizeof == PAGE_SIZE);

////////////////////////////////////////////
// DATABASE PAGE LAYOUT
////////////////////////////////////////////

union DatabasePage
{
	HeapPage heap_page;
}

////////////////////////////////////////////
// DISK MANAGER
////////////////////////////////////////////

interface IDiskManager
{
	void read_page(PageLocation page_location, Page* page);
	void write_page(PageLocation page_location, Page* page);
}

class DiskManager : IDiskManager
{
	private
	{
		static DB_ROOT = "db";
		// Map of (database ID, table ID) to file descriptor
		int[TableLocation] fd_map;
	}

	string get_db_file_path(db_id_t db_id)
	{
		return format("%s/%d", DB_ROOT, db_id);
	}

	string get_table_file_path(TableLocation table_location)
	{
		string db_file_path = get_db_file_path(table_location.db_id);
		return format("%s/%d", db_file_path, table_location.table_id);
	}

	private int open_table(TableLocation table_location)
	{
		infof("Reading table %d from database %d", table_location.table_id, table_location.db_id);

		auto db_dir = get_db_file_path(table_location.db_id);
		auto table_file = get_table_file_path(table_location);

		mkdirRecurse(db_dir);

		auto fd = open(table_file.ptr, O_RDWR | O_CREAT | O_DIRECT, octal!644);
		assert(fd != -1);

		return fd;
	}

	override void read_page(PageLocation page_location, Page* page)
	{
		infof("Reading page %d from table %d in database %d",
			page_location.page_num, page_location.table_id, page_location.db_id);

		auto table_location = TableLocation(page_location.db_id, page_location.table_id);
		if (table_location !in fd_map)
		{
			fd_map[table_location] = open_table(table_location);
		}

		auto fd = fd_map[table_location];
		auto offset = page_location.page_num * PAGE_SIZE;
		auto bytes_read = pread(fd, page, PAGE_SIZE, offset);

		// Special case for existing but empty file -- return all zeros
		if (bytes_read == 0)
		{
			memset(page, 0, PAGE_SIZE);
			return;
		}

		assert(bytes_read == PAGE_SIZE);
	}

	override void write_page(PageLocation page_location, Page* page)
	{
		infof("Writing page %d to table %d in database %d",
			page_location.page_num, page_location.table_id, page_location.db_id);

		auto table_location = TableLocation(page_location.db_id, page_location.table_id);
		if (table_location !in fd_map)
		{
			fd_map[table_location] = open_table(table_location);
		}

		auto fd = fd_map[table_location];
		auto offset = page_location.page_num * PAGE_SIZE;
		auto bytes_written = pwrite(fd, page, PAGE_SIZE, offset);
		assert(bytes_written == PAGE_SIZE);
	}
}

////////////////////////////////////////////
// EVICTION POLICY
////////////////////////////////////////////

interface IEvictionPolicy
{
	frame_idx_t choose_victim_frame(); // Returns the frame index of the victim frame
	void frame_pinned(frame_idx_t frame_idx); // Callback for when a frame is pinned
	void frame_unpinned(frame_idx_t frame_idx); // Callback for when a frame is unpinned
}

class ClockEvictionPolicy : IEvictionPolicy
{
	private
	{
		frame_idx_t clock_hand;
		bool[BUF_POOL_NUM_PAGES] is_pinned;
		bool[BUF_POOL_NUM_PAGES] is_referenced;
	}

	this()
	{
		clock_hand = 0;
	}

	frame_idx_t choose_victim_frame()
	{
		while (true)
		{
			if (!is_pinned[clock_hand] && !is_referenced[clock_hand])
			{
				return clock_hand;
			}

			is_referenced[clock_hand] = false;
			clock_hand = (clock_hand + 1) % BUF_POOL_NUM_PAGES;
		}
	}

	void frame_pinned(frame_idx_t frame_idx)
	{
		is_pinned[frame_idx] = true;
	}

	void frame_unpinned(frame_idx_t frame_idx)
	{
		is_pinned[frame_idx] = false;
	}
}

////////////////////////////////////////////
// BUFFER POOL
////////////////////////////////////////////

interface IBufferPool
{
	Page* fetch_page(PageLocation page_location);
	void unpin_page(PageLocation page_location, bool is_dirty = false);
	void flush_page(PageLocation page_location);
	void flush_all_pages();
}

class BufferPool : IBufferPool
{
	private
	{
		align(PAGE_SIZE) ubyte[PAGE_SIZE * BUF_POOL_NUM_PAGES] data;

		// Struct-of-Arrays for the buffer pool frames metadata
		// Atomic because multiple threads may be accessing the same frame
		int[BUF_POOL_NUM_PAGES] pin_count;
		bool[BUF_POOL_NUM_PAGES] is_dirty;
		bool[BUF_POOL_NUM_PAGES] ref_bit;

		// Free list
		frame_idx_t[] free_list;

		// Lookup table for page IDs -> frame indices (aka the "page table")
		frame_idx_t[PageLocation] page_table;
		PageLocation[frame_idx_t] reverse_page_table; // Used for eviction only, to look up the page location of a frame
		ReadWriteMutex page_table_mutex;

		IEvictionPolicy eviction_policy = new ClockEvictionPolicy();
		IDiskManager disk_manager = new DiskManager();
	}

	this()
	{
		mmap(&data, data.sizeof, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
		page_table_mutex = new ReadWriteMutex();

		// Initialize the free list
		foreach (i; 0 .. BUF_POOL_NUM_PAGES)
		{
			free_list ~= i;
		}
	}

	////////////////////////////////////////////
	// INVARIANTS
	////////////////////////////////////////////

	invariant
	{
		// Pin count is non-negative
		foreach (i; 0 .. BUF_POOL_NUM_PAGES)
		{
			assert(pin_count[i] >= 0);
		}

		// Free list contains only valid frame indices
		foreach (i; 0 .. free_list.length)
		{
			assert(free_list[i] >= 0 && free_list[i] < BUF_POOL_NUM_PAGES);
		}

		// Free list contains no duplicates
		foreach (i; 0 .. free_list.length)
		{
			foreach (j; 0 .. free_list.length)
			{
				if (i != j)
				{
					assert(free_list[i] != free_list[j]);
				}
			}
		}

		// Free list contains no frames that are pinned
		foreach (i; 0 .. free_list.length)
		{
			assert(pin_count[free_list[i]] == 0);
		}

		// Free list contains no frames that are dirty
		foreach (i; 0 .. free_list.length)
		{
			assert(!is_dirty[free_list[i]]);
		}

		// Free list contains no frames that are referenced
		foreach (i; 0 .. free_list.length)
		{
			assert(!ref_bit[free_list[i]]);
		}

		// Page table contains only valid frame indices
		foreach (page_location, frame_idx; page_table)
		{
			assert(frame_idx >= 0 && frame_idx < BUF_POOL_NUM_PAGES);
		}

		// Page table contains no duplicates
		foreach (page_location, frame_idx; page_table)
		{
			foreach (page_location2, frame_idx2; page_table)
			{
				if (page_location != page_location2)
				{
					assert(frame_idx != frame_idx2);
				}
			}
		}
	}

	////////////////////////////////////////////
	// PRIVATE METHODS
	////////////////////////////////////////////

	private Page* get_frame(frame_idx_t frame_idx)
	{
		auto ptr = data.ptr + (frame_idx * PAGE_SIZE);
		return cast(Page*) ptr;
	}

	// Pin a frame
	private void pin_frame(frame_idx_t frame_idx)
	{
		pin_count[frame_idx]++;
		eviction_policy.frame_pinned(frame_idx);
	}

	// Unpin a frame
	private void unpin_frame(frame_idx_t frame_idx)
	{
		pin_count[frame_idx]--;
		eviction_policy.frame_unpinned(frame_idx);
	}

	private void add_page_table_entry(PageLocation page_location, frame_idx_t frame_idx)
	{
		page_table[page_location] = frame_idx;
		reverse_page_table[frame_idx] = page_location;
	}

	private void remove_page_table_entry(PageLocation page_location)
	{
		auto frame_idx = page_table[page_location];
		page_table.remove(page_location);
		reverse_page_table[frame_idx] = PageLocation.init;
	}

	private frame_idx_t pop_free_frame()
	{
		assert(free_list.length > 0);
		frame_idx_t frame_idx = free_list.front;
		free_list.popFront();
		return frame_idx;
	}

	private frame_idx_t get_free_frame()
	{
		if (free_list.length > 0)
		{
			return pop_free_frame();
		}
		else
		{
			return eviction_policy.choose_victim_frame();
		}
	}

	private void add_free_frame(frame_idx_t frame_idx)
	{
		free_list ~= frame_idx;
	}

	////////////////////////////////////////////
	// PUBLIC METHODS
	////////////////////////////////////////////

	override Page* fetch_page(PageLocation page_location)
	{
		// Acquire a read lock (latch) on the page table
		synchronized (page_table_mutex.reader)
		{
			// Check if the page is already in the buffer pool
			if (page_location in page_table)
			{
				// Get the frame index
				auto frame_idx = page_table[page_location];

				// Pin the frame
				pin_frame(frame_idx);

				// Return the frame
				return get_frame(frame_idx);
			}
		}

		// Acquire a write lock (latch) on the page table
		synchronized (page_table_mutex.writer)
		{
			// Get a free frame
			auto frame_idx = get_free_frame();

			// Read the page from disk
			disk_manager.read_page(page_location, get_frame(frame_idx));

			// Add the page to the page table
			add_page_table_entry(page_location, frame_idx);

			// Pin the frame
			pin_frame(frame_idx);

			// Return the frame
			return get_frame(frame_idx);
		}
	}

	override void unpin_page(PageLocation page_location, bool dirty = false)
	{
		// Acquire a read lock (latch) on the page table
		// Get the frame index
		frame_idx_t frame_idx;
		synchronized (page_table_mutex.reader)
		{
			frame_idx = page_table[page_location];
		}

		// Unpin the frame
		unpin_frame(frame_idx);

		// Mark the frame as dirty if necessary
		is_dirty[frame_idx] |= dirty;
	}

	override void flush_page(PageLocation page_location)
	{
		// Acquire a read lock (latch) on the page table
		// Get the frame index
		frame_idx_t frame_idx;
		synchronized (page_table_mutex.reader)
		{
			frame_idx = page_table[page_location];
		}

		// Write the page to disk
		disk_manager.write_page(page_location, get_frame(frame_idx));

		// Mark the frame as clean
		is_dirty[frame_idx] = false;
	}

	override void flush_all_pages()
	{
		// Acquire a read lock (latch) on the page table
		synchronized (page_table_mutex.reader)
		{
			// Iterate over all pages in the page table
			foreach (page_location, frame_idx; page_table)
			{
				// Write the page to disk
				disk_manager.write_page(page_location, get_frame(frame_idx));
			}
		}

		// Mark all frames as clean
		is_dirty[] = false;
	}

}

////////////////////////////////////////////
// HEAP FILE ACCESS
////////////////////////////////////////////

class HeapFile
{
	private
	{
		BufferPool* buffer_pool;
		DiskManager* disk_manager;
		db_id_t db_id;
		table_id_t table_id;
		page_num_t num_pages;
	}

	this(BufferPool* buffer_pool, DiskManager* disk_manager, db_id_t db_id, table_id_t table_id)
	{
		infof("HeapFile: Opening file for database %d, table %d", db_id, table_id);
		this.buffer_pool = buffer_pool;
		this.disk_manager = disk_manager;
		this.db_id = db_id;
		this.table_id = table_id;

		// Get the number of pages in the file
		auto heap_file_path = disk_manager.get_table_file_path(TableLocation(db_id, table_id));
		auto heap_file = DirEntry(heap_file_path);
		this.num_pages = heap_file.statBuf.st_size.to!(uint) / PAGE_SIZE;

		// If the file is empty, add a new page
		infof("HeapFile: File has %d pages", num_pages);
		if (num_pages == 0)
		{
			infof("HeapFile: Adding new page to empty file");
			add_page();
		}
	}

	////////////////////////////////////////////
	// PRIVATE METHODS
	////////////////////////////////////////////

	private PageLocation get_page_location(page_num_t page_num)
	{
		return PageLocation(db_id, table_id, page_num);
	}

	private Page* get_page(page_num_t page_num)
	{
		return buffer_pool.fetch_page(get_page_location(page_num));
	}

	private void unpin_page(page_num_t page_num, bool dirty = false)
	{
		buffer_pool.unpin_page(get_page_location(page_num), dirty);
	}

	private void flush_page(page_num_t page_num)
	{
		buffer_pool.flush_page(get_page_location(page_num));
	}

	private void add_page()
	{
		// Create a new page
		ubyte[PAGE_SIZE] page_data;
		HeapPage* heap_page = cast(HeapPage*) page_data.ptr;
		heap_page.inititalize(num_pages);

		// Write the page to disk
		disk_manager.write_page(get_page_location(num_pages), &page_data);

		// Increment the number of pages
		num_pages++;
	}

	// A "get_page" method used for testing/debugging
	Page* debug_get_page(page_num_t page_num)
	{
		return get_page(page_num);
	}

	////////////////////////////////////////////
	// PUBLIC METHODS
	////////////////////////////////////////////

	void insert_record(ubyte[] record)
	{
		infof("HeapFile: Inserting record of length %d", record.length);
		// Get the last page
		auto page = get_page(num_pages - 1);
		HeapPage* heap_page = cast(HeapPage*) page;

		// Check if the page has enough space for the record
		if (heap_page.get_free_space() >= record.length)
		{
			infof("HeapFile: Inserting record into existing page");

			// Insert the record into the page
			heap_page.insert_record(record);

			// Unpin the page
			unpin_page(num_pages - 1, true);
		}
		else
		{
			infof("HeapFile: Adding new page to insert record");

			// Unpin the page
			unpin_page(num_pages - 1);

			// Create a new page
			ubyte[PAGE_SIZE] page_data;
			HeapPage* new_heap_page = cast(HeapPage*) page_data;

			// Insert the record into the page
			new_heap_page.insert_record(record);

			// Increment the number of pages
			num_pages++;

			// Unpin the page
			unpin_page(num_pages - 1, true);
		}
	}

	void delete_record(RecordID record_id)
	{
		// Get the page
		auto page = get_page(record_id.page_num);
		HeapPage* heap_page = cast(HeapPage*) page;

		// Delete the record
		heap_page.delete_record(record_id.slot_num);

		// Unpin the page
		unpin_page(record_id.page_num, true);
	}

	ubyte[] get_record(RecordID record_id)
	{
		infof("Getting record %d from page %d", record_id.slot_num, record_id.page_num);

		// Get the page
		auto page = get_page(record_id.page_num);
		HeapPage* heap_page = cast(HeapPage*) page;

		// Get the record
		auto record = heap_page.get_record(record_id.slot_num);

		// Unpin the page
		unpin_page(record_id.page_num);

		return record;
	}

	void flush_all_pages()
	{
		// Flush all pages
		buffer_pool.flush_all_pages();
	}
}

////////////////////////////////////////////
// MAIN
////////////////////////////////////////////

void main()
{
	auto disk_manager = new DiskManager();

	// Initialize the buffer pool
	auto buffer_pool = new BufferPool();
	scope (exit)
	{
		buffer_pool.flush_all_pages();
	}

	// Create a heap file
	auto heap_file = new HeapFile(&buffer_pool, &disk_manager, 0, 0);

	// Insert a record
	ubyte[] record = cast(ubyte[]) "Hello, world!".dup;
	heap_file.insert_record(record);

	// Insert another record
	ubyte[] record2 = cast(ubyte[]) "Hello, world! 2".dup;
	heap_file.insert_record(record2);

	// Get the records
	auto first_record = heap_file.get_record(RecordID(0, 0));
	auto second_record = heap_file.get_record(RecordID(0, 1));

	// Print the records
	writeln(cast(string) first_record);
	writeln(cast(string) second_record);

	// Delete the first record
	heap_file.delete_record(RecordID(0, 0));

	// Check that the first record is gone
	auto first_record2 = heap_file.get_record(RecordID(0, 0));
	writeln(cast(string) first_record2);
	assert(first_record2 is null);

	// Check that the second record is still there
	auto second_record2 = heap_file.get_record(RecordID(0, 1));
	writeln(cast(string) second_record2);
}
