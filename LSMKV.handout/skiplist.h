#ifndef SKIPLIST_H
#define SKIPLIST_H

#include <cstdint>
#include <string>
#include <list>
namespace skiplist {
    using key_type = uint64_t;
// using value_type = std::vector<char>;
    using value_type = std::string;
    struct Node
    {
        int level;
        key_type key;
        value_type value;
        Node **forward;

        Node(){}

        Node(key_type k, value_type v,int l,Node *next = NULL)
        {
            key = k;
            value = v;
            level = l;
            forward = new Node *[l];
            for (int i = 0; i < l; i++) forward[i] = next;

        }

        ~Node(){
            if (forward!=NULL)
            {
                delete[] forward;
            }

        }
    };

    class skiplist_type
    {
        // add something here

        int MAXLEVEL = 32;
        int length;
        int level;
        //keys that are equal to the max is invalid
        const unsigned long INVALID = UINT64_MAX;
        double posibility;
        Node *head,*tail;
    public:
        ~skiplist_type();
        int random_level();
        explicit skiplist_type(double p = 0.5);
        void put(key_type key, const value_type &val);
        //std::optional<value_type> get(key_type key) const;
        std::string get(key_type key) const;
        void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list);
        int get_length();
        // for hw1 only
        int query_distance(key_type key) const;
        std::list<std::pair<unsigned long, std::string>> traverse();
    };

} // namespace skiplist

#endif // SKIPLIST_H
