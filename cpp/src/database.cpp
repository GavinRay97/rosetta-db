#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <mutex>
#include <ranges>
#include <shared_mutex>
#include <span>
#include <sys/mman.h>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>

#include <format>
#include <spdlog/spdlog.h>

////////////////////////////////////////////////////////////////////////////////
// CONSTANTS
////////////////////////////////////////////////////////////////////////////////

static const std::filesystem::path DATABASE_PATH = "/tmp/mini-sql-engine";

constexpr size_t PAGE_SIZE        = 4096;
constexpr size_t BUFFER_POOL_SIZE = 1000;

////////////////////////////////////////////////////////////////////////////////
// TYPEDEFS
////////////////////////////////////////////////////////////////////////////////

using lsn_t       = uint64_t;
using txn_id_t    = uint32_t;
using page_id_t   = uint32_t;
using table_id_t  = uint32_t;
using db_id_t     = uint32_t;
using frame_idx_t = size_t;

// Slotted Pages
using slot_id_t = uint16_t;
using offset_t  = uint16_t;
using length_t  = uint16_t;

// Page buffers
using Page      = std::span<std::byte, PAGE_SIZE>;
using PageConst = std::span<const std::byte, PAGE_SIZE>;

// Record buffers
using Record      = std::span<std::byte>;
using RecordConst = std::span<const std::byte>;

using read_lock  = std::shared_lock<std::shared_mutex>;
using write_lock = std::unique_lock<std::shared_mutex>;

////////////////////////////////////////////////////////////////////////////////
// HELPER FUNCTIONS
////////////////////////////////////////////////////////////////////////////////

inline void
hash_combine(std::size_t& seed)
{
}

template<typename T, typename... Rest>
inline void
hash_combine(std::size_t& seed, const T& v, Rest... rest)
{
  std::hash<T> hasher;
  seed ^= hasher(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
  hash_combine(seed, rest...);
}

// Create a string-literal for std::byte
constexpr std::byte
operator""_b(char c)
{
  return std::byte { static_cast<unsigned char>(c) };
}

constexpr std::span<const std::byte>
operator""_b(const char* str, size_t size)
{
  std::byte* bytes = new std::byte[size];
  for (size_t i = 0; i < size; i++)
  {
    bytes[i] = std::byte { static_cast<unsigned char>(str[i]) };
  }
  return std::span(bytes, size);
}

////////////////////////////////////////////////////////////////////////////////
// STRUCTS
////////////////////////////////////////////////////////////////////////////////

enum class PageType : uint8_t
{
  HeapPage  = 0,
  FSMPage   = 1,
  IndexPage = 2,
};

static constexpr std::string
get_page_type_extension(PageType page_type)
{
  switch (page_type)
  {
    case PageType::HeapPage: return "heap";
    case PageType::FSMPage: return "fsm";
    case PageType::IndexPage: return "index";
  }
  std::unreachable();
}

struct PageLocation
{
  PageType   page_type;
  db_id_t    db_id;
  table_id_t table_id;
  page_id_t  page_id;

  auto operator<=>(const PageLocation&) const = default;

  static constexpr size_t hash(const PageLocation& page_location)
  {
    size_t seed = 0;
    hash_combine(seed,
                 page_location.page_type,
                 page_location.db_id,
                 page_location.table_id,
                 page_location.page_id);
    return seed;
  }
};

struct RecordID
{
  page_id_t page_id;
  slot_id_t slot_id;

  auto operator<=>(const RecordID&) const = default;
};

////////////////////////////////////////////////////////////////////////////////
// HEAP PAGE
////////////////////////////////////////////////////////////////////////////////

struct HeapPageHeader
{
  page_id_t page_id;
  lsn_t     lsn;
  uint16_t  num_records;
  uint16_t  free_space_offset;
};

struct HeapPageSlot
{
  offset_t offset;
  length_t length;

  auto operator<=>(const HeapPageSlot&) const = default;
};

union HeapPage
{
  std::byte data[PAGE_SIZE];

  struct
  {
    HeapPageHeader header;
    HeapPageSlot   slots[(PAGE_SIZE - sizeof(HeapPageHeader)) / sizeof(HeapPageSlot)];
  };

  void init(page_id_t page_id);

  std::optional<Record>   get_record(slot_id_t slot_id);
  std::optional<RecordID> insert_record(RecordConst record);
  void                    delete_record(slot_id_t slot_id);

  size_t get_free_space();
  bool   has_enough_free_space(size_t size);
};

static_assert(sizeof(HeapPage) == PAGE_SIZE);

union DatabasePage
{
  HeapPage heap_page;
};

void
HeapPage::init(page_id_t page_id)

{
  SPDLOG_INFO("Initializing page {}", page_id);
  SPDLOG_INFO("  free_space_offset [before] = {}", header.free_space_offset);
  header.page_id           = page_id;
  header.lsn               = 0;
  header.num_records       = 0;
  header.free_space_offset = PAGE_SIZE;
  SPDLOG_INFO("  free_space_offset [after] = {}", header.free_space_offset);
}

std::optional<Record>
HeapPage::get_record(slot_id_t slot_id)
{
  SPDLOG_INFO("Getting record with slot_id {} from page {}", slot_id, header.page_id);

  if (slot_id >= header.num_records)
  {
    return std::nullopt;
  }

  auto slot = slots[slot_id];
  return Record(data + slot.offset, slot.length);
}

std::optional<RecordID>
HeapPage::insert_record(RecordConst record)
{
  SPDLOG_INFO("Inserting record of size {} into page {}", record.size(), header.page_id);

  if (!has_enough_free_space(record.size()))
  {
    SPDLOG_INFO("  Not enough free space in page {}", header.page_id);
    return std::nullopt;
  }

  if (header.free_space_offset == 65523)
  {
    header.free_space_offset = PAGE_SIZE;
  }

  SPDLOG_INFO("header.page_id = {}", header.page_id);
  SPDLOG_INFO("header.num_records = {}", header.num_records);
  SPDLOG_INFO("header.free_space_offset = {}", header.free_space_offset);

  auto     slot_id = header.num_records;
  uint16_t offset  = header.free_space_offset - record.size_bytes();
  uint16_t length  = record.size();
  SPDLOG_INFO(
    "  Inserting record into slot {} at offset {} with length {}", slot_id, offset, length);

  std::memcpy(data + offset, record.data(), record.size());

  slots[slot_id] = { offset, length };
  header.num_records++;
  header.free_space_offset = offset;

  return RecordID { header.page_id, slot_id };
}

void
HeapPage::delete_record(slot_id_t slot_id)
{
  if (slot_id >= header.num_records)
  {
    throw std::runtime_error("Invalid slot id");
  }

  auto slot = slots[slot_id];
  std::memset(data + slot.offset, 0, slot.length);

  slots[slot_id] = { 0, 0 };
  header.num_records--;
}

size_t
HeapPage::get_free_space()
{
  return header.free_space_offset - sizeof(HeapPageHeader) -
         sizeof(HeapPageSlot) * header.num_records;
}

bool
HeapPage::has_enough_free_space(size_t size)
{
  return get_free_space() >= size;
}

////////////////////////////////////////////////////////////////////////////////
// INTERFACES
////////////////////////////////////////////////////////////////////////////////

class IDiskManager
{

public:
  virtual ~IDiskManager() = default;

  virtual void read_page(PageLocation page_location, DatabasePage* page)         = 0;
  virtual void write_page(PageLocation pagpage_locatione_id, DatabasePage* page) = 0;

  virtual std::filesystem::path get_page_filepath(PageLocation page_location) = 0;
};

class IBufferPool
{

public:
  virtual ~IBufferPool() = default;

  virtual DatabasePage* fetch_page(PageLocation page_location)           = 0;
  virtual void          unpin(PageLocation page_location, bool is_dirty) = 0;

  virtual void flush(PageLocation page_location) = 0;
  virtual void flush_all()                       = 0;
};

class IHeapTable
{

public:
  virtual ~IHeapTable() = default;

  virtual std::optional<RecordID> insert_record(RecordConst record) = 0;
  virtual std::optional<Record>   get_record(RecordID record_id)    = 0;
  virtual void                    delete_record(RecordID record_id) = 0;

  virtual void scan(std::function<void(RecordID, RecordConst)> callback) = 0;
};

class IDatabase
{

public:
  virtual ~IDatabase() = default;

  virtual IHeapTable* get_table(table_id_t table_id) = 0;
  virtual IHeapTable* create_table()                 = 0;
};

////////////////////////////////////////////////////////////////////////////////
// DISK MANAGER
////////////////////////////////////////////////////////////////////////////////

class DiskManager : public IDiskManager
{
private:
  std::filesystem::path                          m_db_path;
  std::unordered_map<std::filesystem::path, int> m_fd_cache;
  std::shared_mutex                              m_fd_cache_mutex;

  // Initializes the database directory
  void init_db_dir(const std::filesystem::path& db_path);
  // Gets or creates a file descriptor for the given path
  int get_or_create_fd(const std::filesystem::path& path);

public:
  explicit DiskManager(std::filesystem::path db_path = DATABASE_PATH);

  void read_page(PageLocation page_location, DatabasePage* page) override;
  void write_page(PageLocation page_location, DatabasePage* page) override;

  std::filesystem::path get_page_filepath(PageLocation page_location) override;
};

DiskManager::DiskManager(std::filesystem::path db_path)
  : m_db_path(std::move(db_path))
{
  SPDLOG_INFO("Creating disk manager");
  SPDLOG_INFO("Database path: {}", m_db_path.string());
  init_db_dir(m_db_path);
}

void
DiskManager::init_db_dir(const std::filesystem::path& db_path)
{
  SPDLOG_INFO("Initializing disk manager");
  if (!std::filesystem::exists(db_path))
  {
    SPDLOG_INFO("Creating database directory: {}", db_path.string());
    std::filesystem::create_directories(db_path);
  }
}

std::filesystem::path
DiskManager::get_page_filepath(PageLocation page_location)
{
  SPDLOG_INFO("Getting page file path for ({}:{}:{})",
              page_location.db_id,
              page_location.table_id,
              page_location.page_id);

  auto extension = get_page_type_extension(page_location.page_type);

  return std::format("{}/{}/{}/table.{}",
                     m_db_path.string(),
                     page_location.db_id,
                     page_location.table_id,
                     extension);
}

int
DiskManager::get_or_create_fd(const std::filesystem::path& path)
{
  std::filesystem::path db_path = path;
  // Read lock the cache
  {
    std::shared_lock fd_cache_lock(m_fd_cache_mutex);
    if (m_fd_cache.contains(db_path))
    {
      return m_fd_cache[db_path];
    }
  }
  // Write lock the cache
  {
    std::unique_lock fd_cache_write_lock(m_fd_cache_mutex);
    if (m_fd_cache.contains(db_path))
    {
      return m_fd_cache[db_path];
    }

    // Check if the file exists
    if (!std::filesystem::exists(db_path))
    {
      SPDLOG_INFO("Creating file: {}", db_path.string());
      std::filesystem::create_directories(db_path.parent_path());
    }

    int fd = open(db_path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC | O_DIRECT, 0666);
    if (fd == -1)
    {
      throw std::runtime_error("Failed to open file");
    }

    m_fd_cache[db_path] = fd;
    return fd;
  }
}

void
DiskManager::read_page(PageLocation page_location, DatabasePage* page)
{
  std::filesystem::path path = get_page_filepath(page_location);
  int                   fd   = get_or_create_fd(path);

  ssize_t bytes_read = pread(fd, page, PAGE_SIZE, 0);
  if (bytes_read == 0)
  {
    // Special case for empty files
    std::memset(page, 0, PAGE_SIZE);
    return;
  }
  if (bytes_read != PAGE_SIZE)
  {
    throw std::runtime_error("Failed to read page");
  }
}

void
DiskManager::write_page(PageLocation page_location, DatabasePage* page)
{
  std::filesystem::path path = get_page_filepath(page_location);
  int                   fd   = get_or_create_fd(path);

  ssize_t bytes_written = pwrite(fd, page, PAGE_SIZE, 0);
  if (bytes_written != PAGE_SIZE)
  {
    throw std::runtime_error("Failed to write page");
  }
}

////////////////////////////////////////////////////////////////////////////////
// BUFFER POOL
////////////////////////////////////////////////////////////////////////////////

class BufferPool : public IBufferPool
{
  using page_table_t = std::unordered_map<PageLocation, frame_idx_t, decltype(&PageLocation::hash)>;

private:
  alignas(PAGE_SIZE) std::byte m_buffer[BUFFER_POOL_SIZE][PAGE_SIZE];
  IDiskManager*           m_disk_manager;
  page_table_t            m_page_table;
  std::shared_mutex       m_page_table_mutex;
  bool                    m_is_dirty[BUFFER_POOL_SIZE];
  bool                    m_reference_bit[BUFFER_POOL_SIZE];
  int                     pin_count[BUFFER_POOL_SIZE];
  std::deque<frame_idx_t> m_free_frames;
  frame_idx_t             m_clock_hand = 0;

  DatabasePage* get_page(frame_idx_t frame_idx);
  frame_idx_t   evict_page(); // Evicts a page from either the free list or the clock hand
  void          remove_page_from_page_table(frame_idx_t frame_idx);

public:
  explicit BufferPool(IDiskManager* disk_manager);

  DatabasePage* fetch_page(PageLocation page_location) override;
  void          unpin(PageLocation page_location, bool is_dirty) override;

  void flush(PageLocation page_location) override;
  void flush_all() override;
};

BufferPool::BufferPool(IDiskManager* disk_manager)
  : m_disk_manager(disk_manager)
  , m_page_table(BUFFER_POOL_SIZE, &PageLocation::hash)
  , m_free_frames(BUFFER_POOL_SIZE)
{
  mmap(m_buffer,
       BUFFER_POOL_SIZE * PAGE_SIZE,
       PROT_READ | PROT_WRITE,
       MAP_ANONYMOUS | MAP_PRIVATE,
       -1,
       0);
  for (int i = 0; i < BUFFER_POOL_SIZE; i++)
  {
    m_free_frames.push_back(i);
  }
}

DatabasePage*
BufferPool::get_page(frame_idx_t frame_idx)
{
  return reinterpret_cast<DatabasePage*>(m_buffer[frame_idx]);
}

void
BufferPool::remove_page_from_page_table(frame_idx_t frame_idx)
{
  for (auto it = m_page_table.begin(); it != m_page_table.end(); it++)
  {
    if (it->second == frame_idx)
    {
      m_page_table.erase(it);
      if (m_is_dirty[frame_idx])
      {
        m_disk_manager->write_page(it->first, get_page(frame_idx));
        m_is_dirty[frame_idx] = false;
      }
      return;
    }
  }
}

frame_idx_t
BufferPool::evict_page()
{
  // Evict a page from the free list
  if (!m_free_frames.empty())
  {
    frame_idx_t frame_idx = m_free_frames.front();
    m_free_frames.pop_front();
    return frame_idx;
  }

  // Evict a page from the clock hand
  while (true)
  {
    if (pin_count[m_clock_hand] == 0)
    {
      if (m_reference_bit[m_clock_hand])
      {
        m_reference_bit[m_clock_hand] = false;
      }
      else
      {
        // Remove the page from the page table
        {
          std::unique_lock page_table_lock(m_page_table_mutex);
          remove_page_from_page_table(m_clock_hand);
        }
        return m_clock_hand;
      }
    }
    m_clock_hand = (m_clock_hand + 1) % BUFFER_POOL_SIZE;
  }
}

DatabasePage*
BufferPool::fetch_page(PageLocation page_location)
{
  // Check if the page is already in the buffer pool
  {
    std::shared_lock page_table_lock(m_page_table_mutex);
    if (m_page_table.contains(page_location))
    {
      frame_idx_t frame_idx      = m_page_table[page_location];
      m_reference_bit[frame_idx] = true;
      pin_count[frame_idx]++;
      return get_page(frame_idx);
    }
  }
  // If not, evict a page and load the new page into the buffer pool
  frame_idx_t frame_idx = evict_page();
  m_disk_manager->read_page(page_location, get_page(frame_idx));
  {
    std::unique_lock page_table_lock(m_page_table_mutex);
    m_page_table[page_location] = frame_idx;
  }
  m_reference_bit[frame_idx] = true;
  pin_count[frame_idx]++;
  return get_page(frame_idx);
}

void
BufferPool::unpin(PageLocation page_location, bool is_dirty)
{
  std::shared_lock page_table_lock(m_page_table_mutex);
  frame_idx_t      frame_idx = m_page_table[page_location];
  pin_count[frame_idx]--;
  if (is_dirty)
  {
    m_is_dirty[frame_idx] = true;
  }
}

void
BufferPool::flush(PageLocation page_location)
{
  std::shared_lock page_table_lock(m_page_table_mutex);
  frame_idx_t      frame_idx = m_page_table[page_location];
  if (m_is_dirty[frame_idx])
  {
    m_disk_manager->write_page(page_location, get_page(frame_idx));
    m_is_dirty[frame_idx] = false;
  }
}

void
BufferPool::flush_all()
{
  for (auto& [page_location, frame_idx] : m_page_table)
  {
    flush(page_location);
  }
}

////////////////////////////////////////////////////////////////////////////////
// IHEAP TABLE
////////////////////////////////////////////////////////////////////////////////

class HeapTable : public IHeapTable
{
private:
  IDatabase*   m_database;
  IBufferPool* m_buffer_pool;
  db_id_t      m_db_id;
  table_id_t   m_table_id;
  page_id_t    m_next_page_id;

public:
  HeapTable(IDatabase*   database,
            IBufferPool* buffer_pool,
            db_id_t      db_id,
            table_id_t   table_id,
            page_id_t    next_page_id);

  std::optional<RecordID> insert_record(RecordConst record) override;
  std::optional<Record>   get_record(RecordID record_id) override;
  void                    delete_record(RecordID record_id) override;

  void scan(std::function<void(RecordID, RecordConst)> callback) override;
};

HeapTable::HeapTable(IDatabase*   database,
                     IBufferPool* buffer_pool,
                     db_id_t      db_id,
                     table_id_t   table_id,
                     page_id_t    next_page_id)
  : m_database(database)
  , m_buffer_pool(buffer_pool)
  , m_db_id(db_id)
  , m_table_id(table_id)
  , m_next_page_id(next_page_id)
{
  SPDLOG_INFO("Creating heap table for ({}:{})", db_id, table_id);

  // If the table is empty, create the first page
  if (m_next_page_id == 0)
  {
    PageLocation  page_location = { PageType::HeapPage, m_db_id, m_table_id, m_next_page_id };
    DatabasePage* page          = m_buffer_pool->fetch_page(page_location);
    HeapPage*     heap_page     = &page->heap_page;
    heap_page->init(0);
  }
}

std::optional<RecordID>
HeapTable::insert_record(RecordConst record)
{
  SPDLOG_INFO("Inserting record into ({}:{})", m_db_id, m_table_id);

  PageLocation  page_location = { PageType::HeapPage, m_db_id, m_table_id, m_next_page_id };
  DatabasePage* page          = m_buffer_pool->fetch_page(page_location);
  HeapPage*     heap_page     = &page->heap_page;

  if (!heap_page->has_enough_free_space(record.size_bytes()))
  {
    m_next_page_id++;
    page_location.page_id = m_next_page_id;
    page                  = m_buffer_pool->fetch_page(page_location);
    heap_page             = &page->heap_page;
  }

  slot_id_t slot_id = heap_page->header.num_records;
  RecordID  record_id { .page_id = page_location.page_id, .slot_id = slot_id };

  heap_page->insert_record(record);
  m_buffer_pool->unpin(page_location, true);

  return record_id;
}

std::optional<Record>
HeapTable::get_record(RecordID record_id)
{
  SPDLOG_INFO("Getting record from ({}:{})", m_db_id, m_table_id);

  PageLocation  page_location = { PageType::HeapPage, m_db_id, m_table_id, record_id.page_id };
  DatabasePage* page          = m_buffer_pool->fetch_page(page_location);
  HeapPage*     heap_page     = &page->heap_page;

  if (record_id.slot_id >= heap_page->header.num_records)
  {
    return std::nullopt;
  }

  std::optional<Record> record = heap_page->get_record(record_id.slot_id);
  m_buffer_pool->unpin(page_location, false);

  return record;
}

void
HeapTable::delete_record(RecordID record_id)
{
  SPDLOG_INFO("Deleting record from ({}:{})", m_db_id, m_table_id);

  PageLocation  page_location = { PageType::HeapPage, m_db_id, m_table_id, record_id.page_id };
  DatabasePage* page          = m_buffer_pool->fetch_page(page_location);
  HeapPage*     heap_page     = &page->heap_page;

  if (record_id.slot_id >= heap_page->header.num_records)
  {
    return;
  }

  heap_page->delete_record(record_id.slot_id);
  m_buffer_pool->unpin(page_location, true);
}

void
HeapTable::scan(std::function<void(RecordID, RecordConst)> callback)
{
  SPDLOG_INFO("Scanning records from ({}:{})", m_db_id, m_table_id);

  for (page_id_t page_id = 0; page_id <= m_next_page_id; page_id++)
  {
    PageLocation  page_location = { PageType::HeapPage, m_db_id, m_table_id, page_id };
    DatabasePage* page          = m_buffer_pool->fetch_page(page_location);
    HeapPage*     heap_page     = &page->heap_page;

    for (slot_id_t slot_id = 0; slot_id < heap_page->header.num_records; slot_id++)
    {
      RecordID              record_id { .page_id = page_id, .slot_id = slot_id };
      std::optional<Record> record = heap_page->get_record(slot_id);
      if (record)
      {
        callback(record_id, *record);
      }
    }

    m_buffer_pool->unpin(page_location, false);
  }
}

////////////////////////////////////////////////////////////////////////////////
// DATABASE
////////////////////////////////////////////////////////////////////////////////

class Database : public IDatabase
{
private:
  IBufferPool*                                m_buffer_pool;
  db_id_t                                     m_db_id;
  std::unordered_map<table_id_t, IHeapTable*> m_tables;

public:
  Database(IBufferPool* buffer_pool, db_id_t db_id);

  IHeapTable* get_table(table_id_t table_id) override;
  IHeapTable* create_table() override;
};

Database::Database(IBufferPool* buffer_pool, db_id_t db_id)
  : m_buffer_pool(buffer_pool)
  , m_db_id(db_id)
{
  SPDLOG_INFO("Creating database {}", db_id);
}

IHeapTable*
Database::get_table(table_id_t table_id)
{
  SPDLOG_INFO("Getting table {} from database {}", table_id, m_db_id);

  if (m_tables.contains(table_id))
  {
    return m_tables[table_id];
  }

  return nullptr;
}

IHeapTable*
Database::create_table()
{
  SPDLOG_INFO("Creating table in database {}", m_db_id);

  table_id_t  table_id = m_tables.size();
  IHeapTable* table    = new HeapTable(this, m_buffer_pool, m_db_id, table_id, 0);
  m_tables[table_id]   = table;

  return table;
}

int
main()
{
  DiskManager disk_manager = DiskManager();
  BufferPool  buffer_pool  = BufferPool(&disk_manager);
  Database    database     = Database(&buffer_pool, 0);

  IHeapTable* table = database.create_table();
  table->insert_record({ "Hello, world!"_b });

  table->scan([](RecordID record_id, RecordConst record) {
    SPDLOG_INFO("RecordID: ({}, {})", record_id.page_id, record_id.slot_id);
  });

  table->insert_record({ "Hello, world 123!"_b });

  table->scan([](RecordID record_id, RecordConst record) {
    SPDLOG_INFO("RecordID: ({}, {})", record_id.page_id, record_id.slot_id);
  });
}