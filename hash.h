#ifndef PR1MAIN_HASH_H
#define PR1MAIN_HASH_H

#include "vector.h"

template <typename T>
struct Data {
    string key;
    T value;
    Data* next;

    Data(string k, T v) : key(k), value(v), next(nullptr) {}
};

template <typename T>
struct HashTable {
    Data<T>** buckets; // указатель на массив указателей
    int numBuckets; // ёмкость
    int len; // количество элементов

    HashTable(int initBuckets = 4);
    int size() const;
    int capacity() const;
    size_t hash(string key) const;
    void put(string key, T value);
    T get(string key) const;
    void remove(string key);
    void rehash(int newNumBuckets);
    Vector<string> keys() const;
    bool conatins(string key);
};

template <typename T>
HashTable<T>::HashTable(int initBuckets) {
    numBuckets = initBuckets;
    len = 0;
    buckets = new Data<T>*[numBuckets];
    for (int i = 0; i < numBuckets; i++) {
        buckets[i] = nullptr;
    }
}

template <typename T>
int HashTable<T>::size() const {
    return len;
}

template <typename T>
int HashTable<T>::capacity() const {
    return numBuckets;
}

template <typename T>
size_t HashTable<T>::hash(string key) const {
    size_t hashValue = 0;
    for (char c : key) {
        hashValue = (hashValue * 31) + static_cast<unsigned char>(c);
    }
    return hashValue;
}

template <typename T>
void HashTable<T>::put(string key, T value) {
    if (static_cast<double>(len) / numBuckets >= 0.75) {
        rehash(numBuckets * 2);
    }

    size_t index = hash(key) % numBuckets;

    if (buckets[index] == nullptr) {
        buckets[index] = new Data<T>(key, value);
    } else {  // коллизия
        Data<T>* current = buckets[index];
        if (current->key == key) { // если ключи совпали
            current->value = value;
            return;
        }

        while (current->next != nullptr) {
            if (current->key == key) {
                current->value = value;
                return;
            }
            current = current->next;
        }

        current->next = new Data<T>(key, value);
    }

    len++;
}

template <typename T>
T HashTable<T>::get(string key) const {
    size_t index = hash(key) % numBuckets;

    Data<T>* current = buckets[index];
    while (current != nullptr) {
        if (current->key == key) {
            return current->value;
        }
        current = current->next;
    }
    throw runtime_error("Key not found");
}

template <typename T>
void HashTable<T>::remove(string key) {
    size_t index = hash(key) % numBuckets;

    Data<T>* current = buckets[index];
    Data<T>* prev = nullptr;

    while (current != nullptr) {
        if (current->key == key) {
            if (prev == nullptr) {
                buckets[index] = current->next;
            } else {
                prev->next = current->next;
            }
            delete current;
            len--;
            return;
        }
        prev = current;
        current = current->next;
    }
    throw runtime_error("Key not found");
}

template <typename T>
void HashTable<T>::rehash(int newNumBuckets) {
    Data<T>** newBuckets = new Data<T>*[newNumBuckets];
    for (int i = 0; i < newNumBuckets; i++) {
        newBuckets[i] = nullptr;
    }

    for (int i = 0; i < numBuckets; i++) {
        Data<T>* current = buckets[i];
        while (current != nullptr) {
            Data<T>* temp = current->next;
            size_t newIndex = hash(current->key) % newNumBuckets;
            current->next = newBuckets[newIndex];
            newBuckets[newIndex] = current;
            current = temp;
        }
    }

    delete[] buckets;
    buckets = newBuckets;
    numBuckets = newNumBuckets;
}

template <typename T>
Vector<string> HashTable<T>::keys() const {
    Vector<string> allKeys;

    for (int i = 0; i < numBuckets; i++) {
        Data<T>* current = buckets[i];
        while (current != nullptr) {
            allKeys.pushBack(current->key);
            current = current->next;
        }
    }

    return allKeys;
}

template <typename T>
bool HashTable<T>::conatins(string key) {
    try {
        get(key);
    } catch (runtime_error& e) {
        return false;
    }
    return true;
}

template <typename T>
ostream& operator<<(ostream& os, const HashTable<T>& table) {
    for (int i = 0; i < table.capacity(); i++) {
        Data<T>* current = table.buckets[i];
        if (current != nullptr) {
            os << i << ": ";
            while (current != nullptr) {
                os << current->key << " - " << current->value << ", ";
                current = current->next;
            }
            os << endl;
        }
    }
    return os;
}


#endif //PR1MAIN_HASH_H
