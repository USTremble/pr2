#ifndef PR1MAIN_VECTOR_H
#define PR1MAIN_VECTOR_H

#include <iostream>
#include <string>
using namespace std;

template <typename T>
struct Vector {
    T* data;
    int len;
    int cap;

    Vector(int initCap = 16) { // конструктор
        cap = initCap;
        len = 0;
        data = new T[cap];
    }

    int size();
    void extend();
    void pushBack(T value);
    void insert(int index, T value);
    void remove(int index);
    void set(int index, T value);
    void reverse();
    void resize(int newLen);
    int find(T& value);
    Vector<T> copy();
    T get(int index);
    T* begin();
    T* end();
};

template <typename T>
int Vector<T>::size() {
    return len;
}

template <typename T>
void Vector<T>::extend() {
    int newCap = cap * 2;
    T* newData = new T[newCap];
    for (int i = 0; i < cap; i++) {
        newData[i] = data[i];
    }
    delete[] data;
    data = newData;
    cap = newCap;
}

template <typename T>
void Vector<T>::pushBack(T value) {
    if (static_cast<float>(len) / cap >= 0.5) { // если заполнен >= 50% - увеличиваем объём
        extend();
    }
    data[len] = value;
    len++;
}

template <typename T>
void Vector<T>::insert(int index, T value) {
    if (static_cast<float>(len) / cap >= 0.5) {
        extend();
    }
    for (int i = len; i > index; i--) {
        data[i] = data[i - 1];  // дубль назад
    }
    data[index] = value;
    len++;
}

template <typename T>
void Vector<T>::remove(int index) {
    if (index < 0 || index >= len) {
        throw invalid_argument("Invalid index");
    }

    for (int i = index; i < len - 1; i++) {
        data[i] = data[i + 1]; // дубль вперед
    }
    len--;
}

template <typename T>
void Vector<T>::set(int index, T value) {
    if (index < 0 || index >= len) {
        throw invalid_argument("Invalid index");
    }
    data[index] = value;
}

template <typename T>
void Vector<T>::reverse() {
    for (int i = 0; i < len / 2; i++) {
        T temp = data[i];
        data[i] = data[len - i - 1];
        data[len - i - 1] = temp;
    }
}

//check to bool remake
template <typename T>
int Vector<T>::find(T& value) {
    for (int i = 0; i < len; i++) {
        if (data[i] == value) {
            return i;
        }
    }
    return -1;
}

template <typename T>
Vector<T> Vector<T>::copy() {
    Vector<T> secVector(cap);
    secVector.len = len;

    for (int i = 0; i < len; i++) {
        secVector.data[i] = data[i];
    }

    return secVector;
}

template <typename T>
void Vector<T>::resize(int newLen) {
    len = newLen;
}

template <typename T>
T Vector<T>::get(int index) {
    if (index < 0 ) {
        throw invalid_argument("Invalid index");
    }

    return data[index];
}

template <typename T>
T* Vector<T>::begin() {
    return data;
}

template <typename T>
T* Vector<T>::end() {
    return data + len;
}

template <typename T>
ostream& operator<<(ostream& os, Vector<T>& arr) {
    for (int i = 0; i < arr.size(); i++) {
        os << arr.get(i) << " ";
    }
    os << endl;
    return os;
}


#endif //PR1MAIN_VECTOR_H
