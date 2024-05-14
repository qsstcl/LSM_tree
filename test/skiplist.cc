#include "skiplist.h"
#include <optional>

namespace skiplist {

    int skiplist_type::random_level(){
        int lv = 1;
        while (rand()<(posibility*RAND_MAX))
        {
            lv++;
        }
        return MAXLEVEL > lv ? lv : MAXLEVEL;
    }

    skiplist_type::skiplist_type(double p) {
        posibility = p;
        level = 1;
        length = 0;
        tail = new Node(this->INVALID,"",1);
        head = new Node(0,"",1,tail);

    }
    skiplist_type::~skiplist_type(){
        Node* current = head;
        while (current != nullptr) {
            Node* next_node = current->forward[0];
            delete current;
            current = next_node;
        }
    }
    void skiplist_type::put(key_type key, const value_type &val) {
        if (get(key)!="error!"){
            //found it int the skiplist
            Node *p = head;
            for (int i = level-1; i >= 0; i--) {
                while (p->forward[i]->key < key) {
                    p = p->forward[i];
                }
            }
            p = p->forward[0];
            if (p->key == key) p->value = val;
        }else{
            //did not found it in the skiplist
            Node *p = head;

            int lv = random_level();
            if (lv > level){
                this->head->level = lv;
                Node** newForward = new Node*[lv];
                for (int i = 0; i < level; i++)
                {
                    newForward[i] = head->forward[i];
                }
                delete[] head->forward;
                head->forward = newForward;
                this->tail->level = lv;
                newForward = new Node*[lv];
                for (int i = 0; i < level; i++)
                {
                    newForward[i] = tail->forward[i];
                }
                delete[] tail->forward;
                tail->forward = newForward;
                for (int i = lv-1; i >= level; i--) {
                    this->head->forward[i]=tail;
                }
                level = lv;
            }
            Node *update[level];
            for (int i = level-1; i >= 0; i--)
            {
                while (p->forward[i]->key < key)
                {
                    p = p->forward[i];
                }
                update[i] = p;
            }
            //if insert an existing key, update the value
            Node *newNode = new Node(key,val,lv);
            for (int i = lv-1; i >= 0; i--)
            {
                p = update[i];
                newNode->forward[i] = p->forward[i];
                p->forward[i] = newNode;
            }
            length++;
        }

    }
    std::string skiplist_type::get(key_type key) const {
        Node *p = head;
        for (int i = level-1; i >= 0; i--) {
            while (p->forward[i]->key < key) {
                p = p->forward[i];
            }
        }
        p = p->forward[0];
        if (p->key == key) return p->value;
        else return "error!";
    }
    int skiplist_type::query_distance(key_type key) const {
        int count=0;
        Node *p = head;
        for (int i = level-1; i >= 0; i--) {
            count++;
            while (p->forward[i]->key < key) {
                count++;
                p = p->forward[i];
            }
        }

        return count++;
    }

    int skiplist_type::get_length()
    {
        return length;
    }

    std::list<std::pair<unsigned long, std::string>> skiplist_type::traverse() {
        std::list<std::pair<unsigned long, std::string>> result;
        Node* current = head->forward[0];
        while (current != tail) {
            result.push_back(std::make_pair(current->key, current->value));
            current = current->forward[0];
        }
        return result;
    }
    void skiplist_type::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list)
    {
        Node* p = head;
        for (int i = level - 1; i >= 0; --i) {
            while (p->forward[i] != nullptr && p->forward[i]->key < key1) {
                p = p->forward[i];
            }
        }

        p = p->forward[0];
        while (p != nullptr && p->key <= key2) {
            list.push_back(std::make_pair(p->key, p->value));
            p = p->forward[0];
        }
    }

} // namespace skiplist
